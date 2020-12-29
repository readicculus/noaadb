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

class BatchImageAnnotationInformation(luigi.Task):
    artifacts_root = luigi.Parameter()
    species_filter = luigi.ListParameter()
    image_annotations = None

    def populate_dict(self):
        if self.image_annotations is not None:
            return
        logger = logging.getLogger('luigi-interface')
        logger.info('='*50)
        logger.info('Downloading all relevant image/annotation info')
        s = Session()
        annotations = s.query(Annotation) \
            .join(TrainTestValid, Annotation.eo_event_key == TrainTestValid.eo_event_key) \
            .filter(TrainTestValid.eo_event_key != None) \
            .filter(Annotation.eo_box_id != None) \
            .join(BoundingBox, Annotation.eo_box_id == BoundingBox.id) \
            .filter(not_(BoundingBox.is_point)) \
            .options(joinedload(Annotation.ir_box),
                     joinedload(Annotation.eo_box),
                     joinedload(Annotation.species)).all()

        group_by_key = {}
        filter_by_species = list(self.species_filter)
        for annotation in annotations:
            if len(filter_by_species) == 0 or annotation.species.name not in filter_by_species:
                continue
            if annotation.eo_event_key not in group_by_key:
                group_by_key[annotation.eo_event_key] = {'annotations': []}
            group_by_key[annotation.eo_event_key]['annotations'].append(annotation.to_dict())

        images = s.query(EOImage, TrainTestValid.type) \
            .join(TrainTestValid, EOImage.event_key == TrainTestValid.eo_event_key) \
            .filter(TrainTestValid.eo_event_key != None).all()

        for (image, set_type) in images:
            if image.event_key not in group_by_key:
                continue  # skips background?
            group_by_key[image.event_key]['image'] = image.to_dict()
            group_by_key[image.event_key]['set'] = set_type.name
        s.close()
        self.image_annotations = group_by_key
        logger.info('Download Complete!')
        logger.info('='*50)

    def output(self):
        self.populate_dict()
        out = {}
        for k in self.image_annotations:
            out[k] = luigi.LocalTarget(os.path.join(str(self.artifacts_root), '%s.json' % k))
        return out

    def run(self):
        self.populate_dict()
        logger = logging.getLogger('luigi-interface')
        logger.info('=' * 50)
        logger.info('Writing all relevant image/annotation info')
        logger.info(str(self.artifacts_root))
        outputs = self.output()
        for k in self.image_annotations:
            target = outputs[k]
            content = self.image_annotations[k]
            with target.open('w') as f:
                f.write(json.dumps(content))

        logger.info('Writing Complete!')
        logger.info('=' * 50)

class ExportYoloEODatasetTask(DarknetDatasetTask):

    species_filter = luigi.ListParameter()

    def requires(self):
        batches_task = BatchImageAnnotationInformation()
        luigi.build([batches_task], local_scheduler=True)
        batch_files = batches_task.output()
        for i, k in enumerate(list(batch_files.keys())):
            if i > 100:
                continue

            batch_target = batch_files[k]

            with batch_target.open('r') as f:
                batch_data = json.loads(f.read())
            set_type = TrainTestValidEnum[batch_data['set']]
            out_dir = self.get_output_by_enum(set_type)['dir'].path

            t = GenerateImageChips(event_key = k, batch_data_file = batch_target.path, chip_output_dir=out_dir)
            yield t

    def cleanup(self):
        self._delete_non_image_files(self.output()['train_dir'].path)
        self._delete_non_image_files(self.output()['test_dir'].path)
        self._delete_non_image_files(self.output()['valid_dir'].path)

    def _save_dataset_labels(self, chips_list, annotations, set_type):
        output = self.get_output_by_enum(set_type)
        out_dir = output['dir'].path
        os.makedirs(out_dir, exist_ok=True)
        chip_fp_list = []
        for chip in chips_list:
            chip_fp = chip['chip_fp']
            chip_fn = os.path.basename(chip_fp)

            # chip_fp_list.append(chip_fp) #TODO

            chip_h = chip['h']
            chip_w = chip['w']

            yolo_label_fp = os.path.join(out_dir, '.'.join(chip_fn.split('.')[:-1]) + '.txt')


            chip_fp_list.append(chip_fp)
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

            label_target = luigi.LocalTarget(yolo_label_fp)
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
        input = self.input()
        image_list_by_set = {}
        for image_batch_results in input:
            chips_list_target = image_batch_results['chips_list']
            annotations_target = image_batch_results['annotations']
            batch_data_target = image_batch_results['batch_data_file']
            with annotations_target.open('r') as f:
                annotations = json.loads(f.read())
            with chips_list_target.open('r') as f:
                chips_list = json.loads(f.read())
            with batch_data_target.open('r') as f:
                batch_data = json.loads(f.read())
            set_type = TrainTestValidEnum[batch_data['set']]
            if not set_type in image_list_by_set: image_list_by_set[set_type] = []

            chips_filepaths = self._save_dataset_labels(chips_list, annotations, set_type)
            image_list_by_set[set_type] += chips_filepaths

        for set_type in image_list_by_set:
            targets = self.get_output_by_enum(set_type)
            # write to the image list
            with targets['image_list'].open('w') as f:
                for fp in image_list_by_set[set_type]:
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
