import json
import os

import cv2
import luigi

from core import ForcibleTask
from noaadb.schema.models import TrainTestValidEnum
from pipelines.yolo_dataset_export import chipConfig

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


    def _load_image(self):
        image_data = self.batch_data['image']
        im_path = os.path.join(image_data['directory'], image_data['filename'])
        return cv2.imread(im_path)

    def calculate_chips(self):
        chip_config = chipConfig()
        # if the output does not exist calculate the image chips for the given dimension
        image_data = self.batch_data['image']
        label_data = self.batch_data['annotations']
        tiles = tile_2d_stride(image_data['w'], image_data['h'], chip_config.chip_dim,
                               chip_config.chip_dim, chip_config.chip_stride, chip_config.chip_stride)

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
                unassigned_labels.append(label['eo_box']['id'])

        return tiles_to_labels, unassigned_labels

    def run(self):
        # if not os.path.exists(str(self.output_dir)):
        #     os.makedirs(str(self.output_dir), exist_ok=True)

        chips, unassigned_labels = self.calculate_chips()
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

            chip_info = {'chip_fp': image_target_fp,'h':tile['y2']-tile['y1'], 'w': tile['x2'] - tile['x1']}
            chip_files_saved.append(chip_info)

            # add to annotations artifact
            annotations_artifact[image_target_fp] = labels_in_chip

        with self.output()['annotations'].open('w') as f:
            f.write(json.dumps(annotations_artifact, indent=4))

        with self.output()['chips_list'].open('w') as f:
            f.write(json.dumps(chip_files_saved))
