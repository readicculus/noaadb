import json
import math
import os

import luigi
import cv2
import numpy as np

from core import ForcibleTask
from noaadb import Session
from noaadb.schema.models import IRImage


class DrawBoundingBoxesTask(ForcibleTask):
    input_file = luigi.Parameter()

    def out_fn(self):
        new_path = str(self.input_file).replace('processed_results', 'examples')
        new_path = new_path.replace('.json', '.jpg')
        return new_path

    def output(self):
        return luigi.local_target.LocalTarget(self.out_fn())

    def cleanup(self):
        if self.output().exists():
            self.output().remove()

    def _read_ir_norm(self, fp):
        im = cv2.imread(fp, cv2.IMREAD_ANYDEPTH)
        im_norm = ((im - np.min(im)) / (0.0 + np.max(im) - np.min(im)))
        im_norm = im_norm * 255.0
        im_norm = im_norm.astype(np.uint8)
        return im_norm

    def run(self):
        s = Session()
        ir_image_key = os.path.basename(self.input_file).replace('.json', '')


        ir_image = s.query(IRImage) \
            .filter(IRImage.event_key == ir_image_key).one()
        with open(self.input_file, 'r') as f:
            pt2box_dict = json.loads(f.read())
        s.close()

        im = self._read_ir_norm(os.path.join(ir_image.directory, ir_image.filename))
        color_im = cv2.cvtColor(im, cv2.COLOR_GRAY2RGBA)
        for box_id in pt2box_dict:
            if box_id == 'duplicates': continue
            new_box = pt2box_dict[box_id]['new_box']
            if new_box is None:
                continue
            cv2.rectangle(color_im, (new_box['x1'], new_box['y1']), (new_box['x2'], new_box['y2']), (36, 255, 12, 255), 1)

            old_box = pt2box_dict[box_id]['db_box']
            cv2.rectangle(color_im, (old_box['x1'], old_box['y1']), (old_box['x2'], old_box['y2']), (0, 0, 255, 100), 1)
            # draw line from old to new
            new_x_cent = int(new_box['x1'] + (new_box['x2'] - new_box['x1'])/2)
            new_y_cent = int(new_box['y1'] + (new_box['y2'] - new_box['y1'])/2)
            old_x_cent = int(old_box['x1'] + (old_box['x2'] - old_box['x1'])/2)
            old_y_cent = int(old_box['y1'] + (old_box['y2'] - old_box['y1'])/2)
            dist = math.hypot(old_x_cent - new_x_cent, old_y_cent - new_y_cent)
            cv2.line(color_im, (new_x_cent, new_y_cent),  (old_x_cent, old_y_cent), (0, 0, 255, 100), 1)
            # draw distance between old and new
            fontScale = .3
            fontColor = (0, 0, 255, 100)
            font = cv2.FONT_HERSHEY_SIMPLEX
            lineType = 1
            cv2.putText(color_im, '%d' % dist,(min(im.shape[1],new_x_cent+5),min(im.shape[0],  new_y_cent)),  font,fontScale,fontColor,lineType)

        os.makedirs(os.path.split(self.output().path)[0], exist_ok=True)
        cv2.imwrite(self.output().path, color_im)
