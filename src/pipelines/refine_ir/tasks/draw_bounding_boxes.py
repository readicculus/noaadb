import json
import math
import os

import luigi
import cv2
import numpy as np

from core import ForcibleTask
from noaadb import Session
from noaadb.schema.models import IRImage
from pipelines.refine_ir import pipelineConfig
from pipelines.refine_ir.tasks import *


class DrawBoundingBoxesTask(ForcibleTask):
    output_dir = luigi.Parameter()
    draw_distance = luigi.BoolParameter(default=True)
    output_two_images = luigi.BoolParameter(default=False)
    draw = luigi.BoolParameter(default=False)

    image_key = luigi.Parameter()
    refinement_dict = luigi.DictParameter()
    def out_fn(self):
        cfg = pipelineConfig()
        return os.path.join(cfg.output_root, str(self.output_dir), str(self.image_key) + '.jpg')

    def output(self):
        return luigi.LocalTarget(self.out_fn())

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
        if not self.draw:
            return
        s = Session()

        ir_image = s.query(IRImage) \
            .filter(IRImage.event_key == str(self.image_key)).one()

        s.close()

        im = self._read_ir_norm(os.path.join(ir_image.directory, ir_image.filename))
        color_im = cv2.cvtColor(im, cv2.COLOR_GRAY2RGBA)
        color_im_post = cv2.cvtColor(im, cv2.COLOR_GRAY2RGBA)

        d = json.loads(self.refinement_dict)
        for box_id in d:
            new_box = d[box_id]['post']
            if new_box is None:
                continue
            cv2.rectangle(color_im_post if self.output_two_images else color_im, (new_box['x1'], new_box['y1']), (new_box['x2'], new_box['y2']), (36, 255, 12, 255), 1)

            old_box = d[box_id]['pre']
            cv2.rectangle(color_im, (old_box['x1'], old_box['y1']), (old_box['x2'], old_box['y2']), (0, 0, 255, 100), 1)
            # draw line from old to new
            new_x_cent = int(new_box['x1'] + (new_box['x2'] - new_box['x1'])/2)
            new_y_cent = int(new_box['y1'] + (new_box['y2'] - new_box['y1'])/2)
            old_x_cent = int(old_box['x1'] + (old_box['x2'] - old_box['x1'])/2)
            old_y_cent = int(old_box['y1'] + (old_box['y2'] - old_box['y1'])/2)

            if self.draw_distance:
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
        if self.output_two_images:
            parts = self.output().path.split('.')
            parts[-2] += '_post'
            fn = '.'.join(parts)
            cv2.imwrite(fn, color_im_post)
