import json
import logging
import os

import luigi
from sqlalchemy import not_, desc, asc
from sqlalchemy.orm import joinedload

from noaadb import Session
from noaadb.schema.models import Annotation, TrainTestValid, TrainTestValidEnum, Species, EOImage, BoundingBox, \
    IRVerifiedBackground
from pipelines.yolo_dataset_export import datasetConfig
from pipelines.yolo_dataset_export.tasks import DarknetDatasetTask
from pipelines.yolo_dataset_export.tasks.eo import GenerateImageChips, GenerateBackgroundImageChips


class BatchImageAnnotationInformation(luigi.Task):
    artifacts_root = luigi.Parameter()
    dataset_name = luigi.Parameter()
    image_annotations = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataset_cfg = datasetConfig()

    def populate_dict(self):
        if self.image_annotations is not None:
            return
        logger = logging.getLogger('luigi-interface')
        logger.info('='*50)
        logger.info('Downloading all relevant image/annotation info')
        s = Session()
        cfg_train = self.dataset_cfg.get_cfg(TrainTestValidEnum.train)
        cfg_test = self.dataset_cfg.get_cfg(TrainTestValidEnum.test)
        cfg_valid= self.dataset_cfg.get_cfg(TrainTestValidEnum.valid)
        unique_species = list(set(cfg_train.species_filter + cfg_test.species_filter + cfg_valid.species_filter))

        annotations = s.query(Annotation).join(Species, Annotation.species)\
            .join(TrainTestValid, Annotation.eo_event_key == TrainTestValid.eo_event_key) \
            .filter(Species.name.in_(unique_species)) \
            .filter(TrainTestValid.eo_event_key != None) \
            .filter(Annotation.eo_box_id != None) \
            .join(BoundingBox, Annotation.eo_box_id == BoundingBox.id) \
            .filter(not_(BoundingBox.is_point)) \
            .options(joinedload(Annotation.ir_box),
                     joinedload(Annotation.eo_box)).all()

        group_by_key = {}
        for annotation in annotations:
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
        os.makedirs(os.path.join(str(self.artifacts_root), str(self.dataset_name)), exist_ok=True)
        for k in self.image_annotations:
            out[k] = luigi.LocalTarget(os.path.join(str(self.artifacts_root), str(self.dataset_name), '%s.json' % k))
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


    def requires(self):
        batches_task = BatchImageAnnotationInformation(dataset_name=self.dataset_name)
        luigi.build([batches_task], local_scheduler=True)
        batch_files = batches_task.output()
        num_by_set_type  = {TrainTestValidEnum.train: 0, TrainTestValidEnum.test:0, TrainTestValidEnum.valid: 0}
        for i, k in enumerate(list(batch_files.keys())):
            batch_target = batch_files[k]
            with batch_target.open('r') as f:
                batch_data = json.loads(f.read())
            set_type = TrainTestValidEnum[batch_data['set']]
            num_by_set_type[set_type] += 1
            out_dir = self.get_output_by_enum(set_type)['dir'].path

            t = GenerateImageChips(event_key = k, batch_data_file = batch_target.path, chip_output_dir=out_dir)
            yield t


    def cleanup(self):
        # self._delete_non_image_files(self.output()['train_dir'].path)
        # self._delete_non_image_files(self.output()['test_dir'].path)
        # self._delete_non_image_files(self.output()['valid_dir'].path)
        pass

    def _save_dataset_labels(self, chips_list, annotations, set_type):
        output = self.get_output_by_enum(set_type)
        out_dir = output['dir'].path
        os.makedirs(out_dir, exist_ok=True)
        chip_fp_list = []
        for chip in chips_list:
            chip_fp = chip['chip_fp']
            assert (os.path.isfile(chip_fp))
            chip_fn = os.path.basename(chip_fp)
            chip_fp_list.append(chip_fp)

            chip_h = chip['h']
            chip_w = chip['w']
            yolo_label_fp = os.path.join(out_dir, '.'.join(chip_fn.split('.')[:-1]) + '.txt')
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
                ids = self._get_name_id(a['class_id'], set_type)
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

    def delete_background(self, set_type = None):
        if set_type is None:
            for set_type in TrainTestValidEnum:
                self.delete_background(set_type)
        else:
            bg = self.get_background_images(set_type)
            for (lbl_fp, im_fp) in bg:
                try:
                    os.remove(lbl_fp)
                except OSError:
                    pass
                try:
                    os.remove(im_fp)
                except OSError:
                    pass

            im_list = self.get_output_by_enum(set_type)['image_list']
            non_bg_ims = []
            with im_list.open('r') as f:
                for l in f.readlines():
                    l = l.strip()
                    if os.path.basename(l)[:2] == 'BG':
                        continue
                    non_bg_ims.append(l)
            # with im_list.open('w') as f:
            with im_list.open('w') as f:
                for fp in non_bg_ims:
                    f.write('%s\n'%fp)
    def generate_background(self):
        s = Session()
        background_images = s.query(EOImage.event_key).join(IRVerifiedBackground, IRVerifiedBackground.ir_event_key == EOImage.event_key)\
            .order_by(asc(EOImage.event_key)).all()
        s.close()
        total_bg_frames = len(background_images)
        max_per_set_bg_frames = int(total_bg_frames/3)
        for si, set_type in enumerate(TrainTestValidEnum):
            existing_bg_count = len(self.get_background_images(set_type))
            tasks = []
            set_dir = self.get_output_by_enum(set_type)['dir']
            im_ct = sum(1 for line in open(self.get_output_by_enum(set_type)['image_list'].path))-existing_bg_count
            cfg = self.dataset_cfg.get_cfg(set_type)
            bg_ratio = cfg.background_ratio
            bg_requested= bg_ratio*im_ct
            chips_per_bg = 20
            if chips_per_bg < 20:
                chips_per_bg = 20

            bg_set = background_images[si*max_per_set_bg_frames:si*max_per_set_bg_frames+max_per_set_bg_frames]
            count = existing_bg_count
            for (event_key,) in bg_set:
                if count>bg_requested:
                    break
                task = GenerateBackgroundImageChips(num_to_generate=chips_per_bg, event_key=event_key, output_dir=set_dir.path)
                tasks.append(task)
                count+=chips_per_bg

            luigi.build(tasks, local_scheduler=True)
            images = []
            with open(self.get_output_by_enum(set_type)['image_list'].path, 'r') as f:
                for l in f.readlines():
                    images.append(os.path.basename(l.strip()))
            added_images = 0
            with open(self.get_output_by_enum(set_type)['image_list'].path,'a') as f:
                for task in tasks:
                    chips = task.output()
                    for ci, chip in chips.items():
                        chip_fp = chip['chip'].path
                        if os.path.basename(chip_fp) in images:
                            continue
                        f.write('%s\n' % chip_fp)
                        added_images+=1
            print('Added %d background images' % added_images)

        self.generate_stats()
        self.validate_dataset()

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

            if len(chips_list) > 0:
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
