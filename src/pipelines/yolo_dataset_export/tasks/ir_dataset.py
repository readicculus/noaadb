import logging
import os
import random
import re
import shutil

import cv2
import luigi
import numpy as np

from core import ForcibleTask
from noaadb import Session
from noaadb.schema.models import Annotation, TrainTestValid, TrainTestValidEnum, IRImage, Species
from pipelines.yolo_dataset_export import processingConfig


class ExportYoloIRDatasetTask(ForcibleTask):
    dataset_root = luigi.Parameter()
    dataset_name = luigi.Parameter()
    num_examples = luigi.IntParameter(default=20)
    delete_images_on_rerun = luigi.BoolParameter()

    names = {}
    species_by_db_id = {}


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
        im = cv2.imread(image_path, cv2.IMREAD_UNCHANGED)
        labels_path = '.'.join(image_path.split('.')[:-1]) + '.txt'
        im_w = im.shape[1]
        im_h = im.shape[0]
        color_im = cv2.cvtColor(im, cv2.COLOR_GRAY2RGB)
        with open(labels_path, 'r') as f:
            # <object-class> <x_center> <y_center> <width> <height>
            for l in f.readlines():
                label = l.strip()
                obj_class, x_cent, y_cent, w, h = label.split(' ')
                obj_class, x_cent, y_cent, w, h = \
                    (int(obj_class), float(x_cent),float(y_cent), float(w), float(h))
                aw = w * im_w
                ah = h * im_h
                ax = x_cent * im_w
                ay = y_cent * im_h
                x1 = int(ax - (aw/2))
                x2 = int(ax + (aw/2))
                y1 = int(ay - (ah/2))
                y2 = int(ay + (ah/2))
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


    def _query_annotations_by_type(self, s, type):
        annotations = s.query(Annotation)\
                .join(TrainTestValid, Annotation.ir_event_key==TrainTestValid.ir_event_key)\
                .filter(TrainTestValid.ir_event_key != None)\
                .filter(Annotation.ir_box_id != None)\
                .filter(TrainTestValid.type == type).all()

        group_by_image = {}
        for annotation in annotations:
            if annotation.ir_event_key not in group_by_image:
                group_by_image[annotation.ir_event_key] = []
            group_by_image[annotation.ir_event_key].append(annotation)
        return group_by_image

    def _query_images_by_type(self, s, type):
        images = s.query(IRImage)\
                .join(TrainTestValid, IRImage.event_key==TrainTestValid.ir_event_key)\
                .filter(TrainTestValid.ir_event_key != None)\
                .filter(TrainTestValid.type == type).all()

        image_by_key = {}
        for image in images:
            image_by_key[image.event_key] = image
        return image_by_key

    def _read_ir_norm(self, fp):
        im = cv2.imread(fp, cv2.IMREAD_ANYDEPTH)
        im_norm = ((im - np.min(im)) / (0.0 + np.max(im) - np.min(im)))
        im_norm = im_norm * 255.0
        im_norm = im_norm.astype(np.uint8)
        return im_norm


    def _save_dataset_images(self, images, target_dir, target_list):
        out_ext = '.png'
        os.makedirs(target_dir.path, exist_ok=True)
        image_list = []
        processing_config = processingConfig()
        for im_key, image in images.items():
            target_fn = '.'.join(image.filename.split('.')[:-1]) + out_ext
            image_fp = os.path.join(image.directory, image.filename)
            image_target_fp = os.path.join(target_dir.path, target_fn)
            if not os.path.isfile(image_target_fp):
                im = self._read_ir_norm(image_fp)
                # check if want to resize image
                if processing_config.fix_image_dimension is not None:
                    target_h = processing_config.fix_image_dimension['h']
                    target_w = processing_config.fix_image_dimension['w']
                    h,w = im.shape
                    assert (target_h >= h and target_w >= w)
                    if target_h == h and target_w == w:
                        cv2.imwrite(image_target_fp, im, [int(cv2.IMWRITE_JPEG_QUALITY), 100])
                    else:
                        new_im = np.zeros((target_h,target_w)).astype(np.int)
                        new_im[:h,:w] = im
                        cv2.imwrite(image_target_fp, new_im, [int(cv2.IMWRITE_JPEG_QUALITY), 100])
                else:
                    cv2.imwrite(image_target_fp, im, [int(cv2.IMWRITE_JPEG_QUALITY), 100])
            image_list.append(image_target_fp)

        with target_list.open('w') as f:
            for im_fp in image_list:
                f.write('%s\n'%im_fp)

    def _save_dataset_labels(self, labels, images, target_dir):
        out_ext = '.txt'
        os.makedirs(target_dir.path, exist_ok=True)

        for im_key, image_labels in labels.items():
            image = images[im_key]
            target_fn = '.'.join(image.filename.split('.')[:-1]) + out_ext
            label_target_fp = os.path.join(target_dir.path, target_fn)

            im_w = image.width
            im_h = image.height
            processing_config = processingConfig()
            if processing_config.fix_image_dimension is not None:
                im_h = processing_config.fix_image_dimension['h']
                im_w = processing_config.fix_image_dimension['w']

            yolo_labels = []
            for label in image_labels:
                box = label.ir_box
                box.pad(processing_config.bbox_padding)

                # Make yolo labels
                # <object-class> <x_center> <y_center> <width> <height>
                id = self._get_name_id(self.species_by_db_id[label.species_id])
                rcx = box.cx / im_w
                rcy = box.cy / im_h
                # add padding to bbox
                rw = (box.width) / im_w
                rh = (box.height) / im_h
                yolo_labels.append((id, rcx, rcy, rw, rh))

            label_target = luigi.LocalTarget(label_target_fp)
            with label_target.open('w') as f:
                for l in yolo_labels:
                    line = '%d %.10f %.10f %.10f %.10f\n' % l
                    f.write(line)

    def _get_name_id(self, name):
        mapped_name = name
        processing_config = processingConfig()
        for src,dst in processing_config.species_map.items():
            if src == "*" or src == name:
                mapped_name = dst
                break
        if mapped_name not in self.names:
            id = len(self.names)
            self.names[mapped_name] = id
        return self.names[mapped_name]

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

    def _populate_species_map(self, s):
        species = s.query(Species).all()
        for sp in species:
            self.species_by_db_id[sp.id] = sp.name

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
        s = Session()
        self._populate_species_map(s)
        # Query images and labels
        train_labels = self._query_annotations_by_type(s, TrainTestValidEnum.train)
        train_images = self._query_images_by_type(s, TrainTestValidEnum.train)
        logger.info('Exporting Train Images...')
        self._save_dataset_images(train_images, output['train_dir'], output['train_list'])
        logger.info('Exporting Train Labels...')
        self._save_dataset_labels(train_labels, train_images, output['train_dir'])


        test_labels = self._query_annotations_by_type(s, TrainTestValidEnum.test)
        test_images = self._query_images_by_type(s, TrainTestValidEnum.test)
        logger.info('Exporting Test Images...')
        self._save_dataset_images(test_images, output['test_dir'], output['test_list'])
        logger.info('Exporting Test Labels...')
        self._save_dataset_labels(test_labels, test_images, output['test_dir'])


        valid_labels = self._query_annotations_by_type(s, TrainTestValidEnum.valid)
        valid_images = self._query_images_by_type(s, TrainTestValidEnum.valid)
        logger.info('Exporting Valid Images...')
        self._save_dataset_images(valid_images, output['valid_dir'], output['valid_list'])
        logger.info('Exporting Valid Labels...')
        self._save_dataset_labels(valid_labels, valid_images, output['valid_dir'])
        s.close()

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
