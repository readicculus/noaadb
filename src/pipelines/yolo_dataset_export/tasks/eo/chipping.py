import json
import logging
import os
import random

import cv2
import luigi

from core import ForcibleTask
from noaadb import Session
from noaadb.schema.models import TrainTestValidEnum, EOImage
from pipelines.yolo_dataset_export import chipConfig, datasetConfig, DATASET_LOG_NAME


def tile_2d_stride(m_w, m_h, c_w, c_h, stride_x, stride_y):
        m = m_w // stride_x
        n = m_h // stride_y
        m_remainder = m_w - (m * (c_w))
        n_remainder = m_h - (n * (c_h))

        tiles = []
        for i in range(m + 1):
            for j in range(n + 1):
                x1 = i * (stride_x) if i != m else m_w - c_w
                x2 = x1 + c_w
                y1 = j * (stride_y) if j != n else m_h - c_h
                y2 = y1 + c_h
                tiles.append({'x1': x1, 'x2': x2, 'y1': y1, 'y2': y2, 'i': i, 'j': j})
        return tiles

def percent_on_tile(label, tile) -> float:
    dx = min(label['x2'], tile['x2']) - max(label['x1'], tile['x1'])
    dy = min(label['y2'], tile['y2']) - max(label['y1'], tile['y1'])
    if (dx < 0) or (dy < 0):
        return 0
    intersected_area = dx * dy
    label_area = (label['x2'] - label['x1']) * (label['y2'] - label['y1'])
    return intersected_area / label_area

class GenerateImageChips(ForcibleTask):
    event_key = luigi.Parameter()
    batch_data_file = luigi.Parameter()
    artifacts_root = luigi.Parameter()
    chip_output_dir = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        with open(str(self.batch_data_file), 'r') as f:
            self.batch_data = json.loads(f.read())
        self.dataset_cfg = datasetConfig()
        self._complete = super().complete

    def cleanup(self):
        pass

    def output(self):
        chip_config = chipConfig()
        dir_id = chip_config.get_dir_id()

        image_key = self.batch_data['image']['event_key']
        chips_fp = os.path.join(str(self.artifacts_root), dir_id, "%s_chips.txt" % image_key)
        annotations_fp = os.path.join(str(self.artifacts_root), dir_id, "%s_annotations.json" % image_key)
        return {'chips_list': luigi.LocalTarget(chips_fp),
                'annotations': luigi.LocalTarget(annotations_fp),
                'batch_data_file': luigi.LocalTarget(self.batch_data_file)}

    def complete(self):
        if self.output()['chips_list'].exists():
            with self.output()['chips_list'].open('r') as f:
                v = json.loads(f.read())
                for el in v:
                    if not os.path.isfile(el['chip_fp']):
                        return False

        return self._complete()

    def _load_image(self):
        image_data = self.batch_data['image']
        im_path = os.path.join(image_data['directory'], image_data['filename'])
        return cv2.imread(im_path)

    def calculate_chips(self):
        chip_config = chipConfig()
        # if the output does not exist calculate the image chips for the given dimension
        image_data = self.batch_data['image']
        label_data = self.batch_data['annotations']
        set_name = self.batch_data['set']
        set_type = TrainTestValidEnum[set_name]
        dataset_cfg = self.dataset_cfg.get_cfg(set_type)

        tiles = tile_2d_stride(image_data['w'], image_data['h'], chip_config.chip_w,
                               chip_config.chip_h, chip_config.chip_stride_x,
                               chip_config.chip_stride_y)

        tiles_to_labels = {}

        for tile in tiles:
            tile_key = "%d-%d-%d-%d" % (tile['x1'], tile['y1'], tile['x2'], tile['y2'])

            for label in label_data:
                pct_on_chip = percent_on_tile(label['eo_box'], tile)
                if pct_on_chip > float(chip_config.label_overlap_threshold):
                    if not tile_key in tiles_to_labels:
                        tiles_to_labels[tile_key] = {'tile': tile, 'labels': []}
                    x1, y1, x2, y2 = label['eo_box']['x1'], label['eo_box']['y1'], label['eo_box']['x2'], \
                                     label['eo_box']['y2']
                    # make box relative to its crop
                    x1 -= tile['x1']
                    x2 -= tile['x1']
                    y1 -= tile['y1']
                    y2 -= tile['y1']
                    # ensure box is in crop bounds
                    if x1 < 0: x1 = 0
                    if y1 < 0: y1 = 0
                    tile_w = tile['x2'] - tile['x1']
                    tile_h = tile['y2'] - tile['y1']
                    if x2 > tile_w:
                        x2 = tile_w
                    if y2 > tile_h:
                        y2 = tile_h
                    r = {'box_id': label['eo_box']['id'], 'percent_on_chip': pct_on_chip, 'x1': x1, 'y1': y1, 'x2': x2,
                         'y2': y2, 'class_id': label['species']}
                    tiles_to_labels[tile_key]['labels'].append(r)

        # if any chips with a species that is not 'allowed'aka (remove_chip_if_species_present) remove the chip
        remove_chip_if_species_present = list(dataset_cfg.remove_chip_if_species_present)
        to_remove_keys = set()
        for tile_k in tiles_to_labels:
            for label in tiles_to_labels[tile_k]['labels']:
                if label['class_id'] in remove_chip_if_species_present:
                    to_remove_keys.add(tile_k)
        for tile_k in to_remove_keys:
            del tiles_to_labels[tile_k]
        # see if any labels did not make it onto a tile
        unassigned_labels = []
        for label in label_data:
            found = False
            for tile_key in tiles_to_labels:
                for label_id in tiles_to_labels[tile_key]['labels']:
                    if label['eo_box']['id'] == label_id['box_id']:
                        found = True
                        break
            if not found:
                unassigned_labels.append(label)

        return tiles_to_labels, unassigned_labels

    def run(self):
        # if not os.path.exists(str(self.output_dir)):
        #     os.makedirs(str(self.output_dir), exist_ok=True)

        chips, unassigned_labels = self.calculate_chips()
        dataset_log = logging.getLogger(DATASET_LOG_NAME)
        if len(unassigned_labels) > 0:
            dataset_log.info('%d labels were not assigned to a chip and are therefor unrepresented in the generated dataset.' % len(unassigned_labels))
            for l in unassigned_labels:
                dataset_log.info(json.dumps(l))

        im = self._load_image()
        chip_files_saved = []
        annotations_artifact = {}
        for chip in chips.values():
            tile = chip['tile']
            labels_in_chip = chip['labels']

            # save image chip
            image_key = self.batch_data['image']['event_key']
            chip_fn = "%s_%d_%d_%d_%d.jpg" % (image_key, tile['x1'], tile['y1'], tile['x2'], tile['y2'])
            image_target_fp = os.path.join(str(self.chip_output_dir), chip_fn)
            if not os.path.isfile(image_target_fp):
                im_chipped = im[tile['y1']:tile['y2'], tile['x1']:tile['x2'], ]
                cv2.imwrite(image_target_fp, im_chipped, [int(cv2.IMWRITE_JPEG_QUALITY), 100])
            else:
                dataset_log.info('File already exists: %s' % image_key)

            if not os.path.isfile(image_target_fp):
                raise Exception('Error creating %s'%image_target_fp)
            chip_info = {'chip_fp': image_target_fp,'h':tile['y2']-tile['y1'], 'w': tile['x2'] - tile['x1']}
            chip_files_saved.append(chip_info)

            # add to annotations artifact
            annotations_artifact[image_target_fp] = labels_in_chip

        with self.output()['annotations'].open('w') as f:
            f.write(json.dumps(annotations_artifact, indent=4))

        with self.output()['chips_list'].open('w') as f:
            f.write(json.dumps(chip_files_saved))


class GenerateBackgroundImageChips(ForcibleTask):
    num_to_generate = luigi.IntParameter()
    event_key = luigi.Parameter()
    output_dir = luigi.Parameter()

    def output(self):
        targets = {}
        for i in range(self.num_to_generate):
            fn_chip = os.path.join(str(self.output_dir), 'BG_%s_%d.jpg' % (str(self.event_key), i))
            fn_label = os.path.join(str(self.output_dir), 'BG_%s_%d.txt' % (str(self.event_key), i))
            targets[i] = {'chip': luigi.LocalTarget(fn_chip), 'label': luigi.LocalTarget(fn_label)}
        return targets

    def cleanup(self):
        for t in self.output():
            for k in self.output()[t]:
                if self.output()[t][k].exists():
                    self.output()[t][k].remove()

    def run(self):
        chip_config = chipConfig()
        s = Session()
        db_im = s.query(EOImage).filter(EOImage.event_key == self.event_key).one()
        s.close()

        tiles = tile_2d_stride(db_im.width, db_im.height, chip_config.chip_w,
                               chip_config.chip_h, chip_config.chip_w,
                               chip_config.chip_h)
        random.shuffle(tiles)
        tiles = tiles[0:self.num_to_generate]

        im = db_im.ocv_load()
        outputs = self.output()
        for i, tile in enumerate(tiles):
            im_chipped = im[tile['y1']:tile['y2'], tile['x1']:tile['x2'], ]
            cv2.imwrite(outputs[i]['chip'].path, im_chipped, [int(cv2.IMWRITE_JPEG_QUALITY), 100])
            with outputs[i]['label'].open('w'): pass # create empty label file
