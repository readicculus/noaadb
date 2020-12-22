import json
import logging
import logging
import os
import random
import shutil

import cv2
import luigi
import numpy as np
from sqlalchemy import not_
from sqlalchemy.orm import joinedload

from core import ForcibleTask
from noaadb import Session
from noaadb.schema.models import Annotation, TrainTestValid, TrainTestValidEnum, Species, EOImage, BoundingBox
from pipelines.yolo_dataset_export.tasks.eo import GenerateImageChips


class ExportYoloEODatasetTask(ForcibleTask):
    dataset_root = luigi.Parameter()
    dataset_name = luigi.Parameter()
    species_map = luigi.DictParameter()
    darknet_path = luigi.Parameter()
    bbox_padding = luigi.IntParameter(default=0)
    num_examples = luigi.IntParameter(default=20)
    delete_images_on_rerun = luigi.BoolParameter()
    species_filter = luigi.ListParameter()
    names = {}
    species_by_db_id = {}

    def _query_image_annotations_by_type(self, s, type):
        annotations = s.query(Annotation) \
            .join(TrainTestValid, Annotation.eo_event_key == TrainTestValid.eo_event_key) \
            .filter(TrainTestValid.eo_event_key != None) \
            .filter(Annotation.eo_box_id != None) \
            .join(BoundingBox, Annotation.eo_box_id == BoundingBox.id) \
            .filter(not_(BoundingBox.is_point)) \
            .filter(TrainTestValid.type == type)\
            .options(joinedload(Annotation.ir_box),
                     joinedload(Annotation.eo_box),
                     joinedload(Annotation.species)).all()

        group_by_key = {}
        filter_by_species = list(self.species_filter)
        for annotation in annotations:
            if len(filter_by_species) == 0 or annotation.species.name not in filter_by_species:
                continue
            if annotation.eo_event_key not in group_by_key:
                group_by_key[annotation.eo_event_key]= {'annotations': []}
            group_by_key[annotation.eo_event_key]['annotations'].append(annotation.to_dict())

        images = s.query(EOImage) \
            .join(TrainTestValid, EOImage.event_key == TrainTestValid.eo_event_key) \
            .filter(TrainTestValid.eo_event_key != None) \
            .filter(TrainTestValid.type == type).all()

        for image in images:
            if image.event_key not in group_by_key:
                continue
            group_by_key[image.event_key]['image'] = image.to_dict()
            group_by_key[image.event_key]['type'] = type

        return group_by_key



    def requires(self):
        targets_to_type = {TrainTestValidEnum.train: self.output()['train_dir'],
                          TrainTestValidEnum.test: self.output()['test_dir'],
                          TrainTestValidEnum.valid: self.output()['valid_dir']}

        list_to_type = {TrainTestValidEnum.train: "train_list",
                       TrainTestValidEnum.test: "test_list",
                       TrainTestValidEnum.valid: "valid_list"}

        s = Session()
        tasks = {}
        for set_type, target in targets_to_type.items():
            tasks[list_to_type[set_type]] = []
            set_items= self._query_image_annotations_by_type(s, set_type)
            s.close()
            for i, (im_key, value) in enumerate(set_items.items()):
                # if i > 100:
                #     break
                image = value['image']
                annotations = value['annotations']
                t = GenerateImageChips(input_image_details = image, input_image_labels = annotations, output_dir = targets_to_type[set_type].path)
                tasks[list_to_type[set_type]].append(t)
        return tasks

    def output(self):
        dataset_dir = os.path.join(str(self.dataset_root), str(self.dataset_name))
        train_dir = os.path.join(dataset_dir, 'train')
        train_list = os.path.join(train_dir, 'images.txt')
        test_dir = os.path.join(dataset_dir, 'test')
        test_list = os.path.join(test_dir, 'images.txt')
        valid_dir = os.path.join(dataset_dir, 'valid')
        valid_list = os.path.join(valid_dir, 'images.txt')
        yolo_data_file = os.path.join(dataset_dir, 'yolo.data')
        yolo_names_file = os.path.join(dataset_dir, 'yolo.names')
        backup_dir = os.path.join(dataset_dir, 'backup')
        examples_dir = os.path.join(dataset_dir, 'examples')

        scripts_dir = os.path.join(dataset_dir, 'scripts')
        train_script = os.path.join(scripts_dir, 'train.sh')

        return {'dataset_dir': luigi.LocalTarget(dataset_dir),
                'train_dir': luigi.LocalTarget(train_dir),
                'test_dir': luigi.LocalTarget(test_dir),
                'valid_dir': luigi.LocalTarget(valid_dir),
                'train_list': luigi.LocalTarget(train_list),
                'test_list': luigi.LocalTarget(test_list),
                'valid_list': luigi.LocalTarget(valid_list),
                'yolo_data_file': luigi.LocalTarget(yolo_data_file),
                'yolo_names_file': luigi.LocalTarget(yolo_names_file),
                'backup_dir': luigi.LocalTarget(backup_dir),
                'examples_dir': luigi.LocalTarget(examples_dir),
                'train_script': luigi.LocalTarget(train_script)}

    def _delete_dir(self, directory):
        if os.path.exists(directory):
            shutil.rmtree(directory)

    def cleanup(self):
        self._delete_dir(self.output()['examples_dir'].path)
        if self.delete_images_on_rerun:
            self._delete_dir(self.output()['train_dir'].path)
            self._delete_dir(self.output()['test_dir'].path)
            self._delete_dir(self.output()['valid_dir'].path)


    def _draw_labels(self, image_path):
        return self.draw_labels(image_path)

    @staticmethod
    def draw_labels(image_path):
        color_im = cv2.imread(image_path, cv2.IMREAD_UNCHANGED)
        labels_path = '.'.join(image_path.split('.')[:-1]) + '.txt'
        im_w = color_im.shape[1]
        im_h = color_im.shape[0]
        with open(labels_path, 'r') as f:
            # <object-class> <x_center> <y_center> <width> <height>
            for l in f.readlines():
                label = l.strip()
                obj_class, x_cent, y_cent, w, h = label.split(' ')
                obj_class, x_cent, y_cent, w, h = \
                    (int(obj_class), float(x_cent), float(y_cent), float(w), float(h))
                aw = w * im_w
                ah = h * im_h
                ax = x_cent * im_w
                ay = y_cent * im_h
                x1 = int(ax - (aw / 2))
                x2 = int(ax + (aw / 2))
                y1 = int(ay - (ah / 2))
                y2 = int(ay + (ah / 2))
                cv2.rectangle(color_im, (x1, y1), (x2, y2), (0, 255, 0), thickness=1)
        return color_im

    def generate_examples(self, list_name, num_to_draw):
        output = self.output()
        examples_dir = output['examples_dir'].path
        out_dir = os.path.join(examples_dir, list_name)
        if os.path.exists(out_dir):
            shutil.rmtree(out_dir)
        os.makedirs(out_dir)

        images = []
        with output[list_name].open('r') as f:
            for line in f.readlines():
                images.append(line.strip())

        selected = random.sample(images, num_to_draw)
        for image_fp in selected:
            out_path = os.path.join(out_dir, os.path.basename(image_fp))
            im = self._draw_labels(image_fp)
            cv2.imwrite(out_path, im)


    def _save_dataset_labels(self, chips_list, annotations):
        chip_fp_list = []
        for chip in chips_list:
            chip_fp = chip['chip_fp']
            chip_fp_list.append(chip_fp)
            chip_h = chip['h']
            chip_w = chip['w']

            yolo_label_fn = '.'.join(chip_fp.split('.')[:-1]) + '.txt'

            chip_annotations = annotations[chip_fp]
            yolo_labels = []
            for a in chip_annotations:
                x1, y1, x2, y2 = a['x1'], a['y1'], a['x2'], a[
                    'y2']
                w = (x2-x1)
                h = (y2-y1)
                cx = x1 + w/2
                cy = y1 + h/2

                # Make yolo labels
                # <object-class> <x_center> <y_center> <width> <height>
                ids = self._get_name_id(a['class_id'])
                rcx = cx / chip_w
                rcy = cy / chip_h
                # add padding to bbox
                rw = (w) / chip_w
                rh = (h) / chip_h
                for species_id in ids:
                    yolo_labels.append((species_id, rcx, rcy, rw, rh))

            label_target = luigi.LocalTarget(yolo_label_fn)
            with label_target.open('w') as f:
                for l in yolo_labels:
                    line = '%d %.10f %.10f %.10f %.10f\n' % l
                    f.write(line)
        return chip_fp_list

    def _get_name_id(self, name):
        mapped_names = []
        if "*" not in self.species_map and name not in self.species_map:
            mapped_names.append(name)
        else:
            for src,dsts in self.species_map.items():
                if src == "*" or src == name:
                    for dst in dsts:
                        mapped_names.append(dst)
                    break

        for mn in mapped_names:
            if mn not in self.names:
                id = len(self.names)
                self.names[mn] = id
        return [self.names[mn] for mn in mapped_names]

    def _save_names_file(self):
        names_in_order = [k for k, v in sorted(self.names.items(), key=lambda item: item[1])]
        with self.output()['yolo_names_file'].open('w') as f:
            for name in names_in_order:
                f.write('%s\n'%name)

    def _save_data_file(self):
        output = self.output()
        data_file = "classes = %d\n" \
                    "train = %s\n" \
                    "test = %s\n" \
                    "valid = %s\n" \
                    "names = %s\n" \
                    "backup = %s\n" % (len(self.names),
                                       output['train_list'].path,
                                       output['test_list'].path,
                                       output['valid_list'].path,
                                       output['yolo_names_file'].path,
                                       output['backup_dir'].path)
        with output['yolo_data_file'].open('w') as f:
            f.write(data_file)

    def run(self):
        """ Generates a IR image dataset for training with the darknet framework

            Directory structure:
            dataset_root/
                train/
                    images.txt
                    images
                test/
                    images.txt
                    images
                valid/
                    images.txt
                    images
                weights/
                meta/
                    examples/
                        - a few example images with bounding boxes to ensure that yolo label format is correct
                    - dataset information/date/number in each set/etc..
                    - darknet shell scripts
                        train.sh -config config.cfg (use DARKNET_PATH environ variable)
                        test.sh -config config.cfg
                yolo.data
                names.txt



        """

        logger = logging.getLogger('luigi-interface')
        logger.info('='*50)
        logger.info('BEGIN EXPORTING YOLO DATASET')
        output = self.output()
        input = self.input()
        for set_key in input:
            image_set = input[set_key]
            set_filepaths = []
            for chipper in image_set:
                chips_list_target = chipper['chips_list']
                annotations_target = chipper['annotations']
                with annotations_target.open('r') as f:
                    annotations = json.loads(f.read())
                with chips_list_target.open('r') as f:
                    chips_list = json.loads(f.read())
                chips_filepaths = self._save_dataset_labels(chips_list, annotations)
                set_filepaths += chips_filepaths
            # write to the image list
            with output[set_key].open('w') as f:
                for fp in set_filepaths:
                    f.write('%s\n' % fp)

        self._save_names_file()
        self._save_data_file()
        os.makedirs(self.output()['backup_dir'].path, exist_ok=True)

        logger.info('Generating metadata...')
        logger.info('Generating examples...')

        logger.info('EXPORT COMPLETE')
        logger.info('='*50)

        self.generate_examples('train_list', self.num_examples)
        self.generate_examples('test_list', self.num_examples)
        self.generate_examples('valid_list', self.num_examples)
