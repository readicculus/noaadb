import logging
import os

import cv2
import luigi
import numpy as np

from noaadb import Session
from noaadb.schema.models import Annotation, TrainTestValid, TrainTestValidEnum, IRImage, IRWithoutErrors, \
    IRVerifiedBackground
from pipelines.yolo_dataset_export import processingConfig
from pipelines.yolo_dataset_export.tasks import DarknetDatasetTask


class ExportYoloIRDatasetTask(DarknetDatasetTask):

    def _query_annotations_by_type(self, s, type):
        q = s.query(Annotation) \
            .join(TrainTestValid, Annotation.ir_event_key == TrainTestValid.ir_event_key) \
            .filter(TrainTestValid.ir_event_key != None).join(Annotation.species) \
            .filter(Annotation.ir_box_id != None).filter(TrainTestValid.type == type)

        cfg = self.dataset_cfg.get_cfg(type)
        if cfg.only_manual_reviewed:
            q = q.join(IRWithoutErrors, IRWithoutErrors.ir_event_key == Annotation.ir_event_key)

        annotations = q.all()

        group_by_image = {}
        for annotation in annotations:
            if annotation.ir_event_key not in group_by_image:
                group_by_image[annotation.ir_event_key] = []
            group_by_image[annotation.ir_event_key].append(annotation)
        return group_by_image

    def _query_images_by_type(self, s, type):
        q = s.query(IRImage) \
            .join(TrainTestValid, IRImage.event_key == TrainTestValid.ir_event_key) \
            .join(Annotation, IRImage.event_key == Annotation.ir_event_key) \
            .filter(TrainTestValid.ir_event_key != None) \
            .filter(TrainTestValid.type == type)

        cfg = self.dataset_cfg.get_cfg(type)
        if cfg.only_manual_reviewed:
            q = q.join(IRWithoutErrors, IRWithoutErrors.ir_event_key == IRImage.event_key)

        images = q.distinct(IRImage.event_key).all()

        # query background_images
        if cfg.background_ratio > 0:
            max_bg = int(cfg.background_ratio * len(images))
            background = s.query(IRImage) \
                .join(TrainTestValid, IRImage.event_key == TrainTestValid.ir_event_key) \
                .join(IRVerifiedBackground, IRImage.event_key == IRVerifiedBackground.ir_event_key) \
                .filter(TrainTestValid.ir_event_key != None) \
                .filter(TrainTestValid.type == type).limit(max_bg).all()
            images = images + background

        image_by_key = {}
        for image in images:
            image_by_key[image.event_key] = image
        return image_by_key

    def _save_dataset_images(self, images, target_dir, target_list):
        out_ext = '.png'
        os.makedirs(target_dir.path, exist_ok=True)
        image_list = []
        for im_key, image in images.items():
            target_fn = '.'.join(image.filename.split('.')[:-1]) + out_ext
            image_target_fp = os.path.join(target_dir.path, target_fn)
            if not os.path.isfile(image_target_fp):
                im = image.ocv_load_normed()
                # check if want to resize image
                if self.processing_cfg.fix_image_dimension is not None:
                    target_h = self.processing_cfg.fix_image_dimension['h']
                    target_w = self.processing_cfg.fix_image_dimension['w']
                    h, w = im.shape
                    assert (target_h >= h and target_w >= w)
                    if target_h == h and target_w == w:
                        cv2.imwrite(image_target_fp, im, [int(cv2.IMWRITE_JPEG_QUALITY), 100])
                    else:
                        new_im = np.zeros((target_h, target_w)).astype(np.int)
                        new_im[:h, :w] = im
                        cv2.imwrite(image_target_fp, new_im, [int(cv2.IMWRITE_JPEG_QUALITY), 100])
                else:
                    cv2.imwrite(image_target_fp, im, [int(cv2.IMWRITE_JPEG_QUALITY), 100])
            image_list.append(image_target_fp)

        with target_list.open('w') as f:
            for im_fp in image_list:
                f.write('%s\n' % im_fp)

    def _save_dataset_labels(self, labels, images, target_dir, set_type):
        out_ext = '.txt'
        os.makedirs(target_dir.path, exist_ok=True)
        cfg = self.dataset_cfg.get_cfg(set_type)
        for im_key in images:
            image = images[im_key]
            image_labels = [] if im_key not in labels else labels[im_key]
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
                box.pad(cfg.bbox_padding)

                # Make yolo labels
                # <object-class> <x_center> <y_center> <width> <height>
                ids = self._get_name_id(label.species_name)
                rcx = box.cx / im_w
                rcy = box.cy / im_h
                rw = (box.width) / im_w
                rh = (box.height) / im_h
                for id in ids:
                    yolo_labels.append((id, rcx, rcy, rw, rh))

            label_target = luigi.LocalTarget(label_target_fp)
            with label_target.open('w') as f:
                for l in yolo_labels:
                    line = '%d %.10f %.10f %.10f %.10f\n' % l
                    f.write(line)





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
        logger.info('=' * 50)
        logger.info('BEGIN EXPORTING YOLO DATASET')

        s = Session()
        # Process each set(Test/train/valid) one by one
        for set_name in self.output_targets:
            set_type = self.output_targets[set_name]['type']
            set_dir = self.output_targets[set_name]['dir']
            set_images = self.output_targets[set_name]['image_list']
            labels =  self._query_annotations_by_type(s, set_type)
            images = self._query_images_by_type(s, set_type)
            logger.info('Exporting %s Images...' % set_name)
            self._save_dataset_images(images, set_dir, set_images)
            logger.info('Exporting %s Labels...' % set_name)
            self._save_dataset_labels(labels, images, set_dir, set_type)

        s.close()

        self._save_names_file()
        self._save_data_file()
        os.makedirs(self.output()['backup_dir'].path, exist_ok=True)

        logger.info('Generating metadata...')
        logger.info('Generating examples...')

        logger.info('EXPORT COMPLETE')
        logger.info('=' * 50)

        self.generate_examples('train_list', self.num_examples)
        self.generate_examples('test_list', self.num_examples)
        self.generate_examples('valid_list', self.num_examples)
