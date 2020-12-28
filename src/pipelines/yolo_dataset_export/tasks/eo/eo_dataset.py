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
from pipelines.yolo_dataset_export.tasks import DarknetDatasetTask
from pipelines.yolo_dataset_export.tasks.eo import GenerateImageChips


class ExportYoloEODatasetTask(DarknetDatasetTask):

    species_filter = luigi.ListParameter()

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
                t = GenerateImageChips(input_image_details = image, input_image_labels = annotations,
                                       output_dir = targets_to_type[set_type].path, set_type=set_type)
                tasks[list_to_type[set_type]].append(t)
        return tasks


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


    def run(self):
        """ Generates a EO image dataset for training with the darknet framework

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
