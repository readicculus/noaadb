import copy
import json
import math
import os
from typing import List

import luigi
from sqlalchemy import func

from core import ForcibleTask
from noaadb import Session
from noaadb.schema.models import IRImage, Annotation, BoundingBox

import cv2
import numpy as np

from pipelines.pt2box.tasks import DrawBoundingBoxesTask, StatsPt2BoxTask


class LabelPointToBoxTransform(object):
    def __init__(self, dx: int, dy: int):
        self.dx = dx
        self.dy = dy

    def __call__(self, labels: List[BoundingBox]) -> List[BoundingBox]:
        box_labels = []
        for l in labels:
            l_copy = copy.copy(l)
            l_copy.x1 -= self.dx
            l_copy.x2 += self.dx
            l_copy.y1 -= self.dy
            l_copy.y2 += self.dy
            box_labels.append(l_copy)
        return box_labels



class Pt2BoxTask(ForcibleTask):
    ir_image_key = luigi.Parameter()
    output_root = luigi.Parameter()

    delta_crop = luigi.IntParameter(default=10)
    percentile = luigi.IntParameter(default=90)
    intersected_point_delta = luigi.IntParameter(default=4)
    draw_result = luigi.BoolParameter(default=False)

    def requires(self):
        pass

    def output(self):
        out_fn = "%s.json" % str(self.ir_image_key)
        out_fp = os.path.join(self.output_root, out_fn)
        return luigi.LocalTarget(out_fp)

    def cleanup(self):
        output = self.output()
        if output.exists():
            output.remove()

    # Helper Methods
    def _read_ir_norm(self, fp):
        im = cv2.imread(fp, cv2.IMREAD_ANYDEPTH)
        im_norm = ((im - np.min(im)) / (0.0 + np.max(im) - np.min(im)))
        im_norm = im_norm * 255.0
        im_norm = im_norm.astype(np.uint8)
        return im_norm

    def _intersection(self, crop, point):
        pt_x1 = point.x1 - self.intersected_point_delta
        pt_x2 = point.x2 + self.intersected_point_delta
        pt_y1 = point.y1 - self.intersected_point_delta
        pt_y2 = point.y2 + self.intersected_point_delta

        crop_x1, crop_y1, crop_x2, crop_y2 = crop

        x1 = max(crop_x1, pt_x1)
        y1 = max(crop_y1, pt_y1)
        x2 = min(crop_x2, pt_x2)
        y2 = min(crop_y2, pt_y2)
        if x2 < x1 or y2 < y1:
            return None
        intersection_area = (x2 - x1) * (y2 - y1)
        return (x1, y1), (x2, y2)

    def _load_annotations(self):
        s = Session()
        ir_image = s.query(IRImage) \
            .filter(IRImage.event_key == self.ir_image_key).one()
        annotations = s.query(Annotation) \
            .filter(Annotation.ir_event_key == self.ir_image_key).all()
        boxes = [a.ir_box for a in annotations]
        s.close()
        return ir_image, boxes

    def _get_crop(self, point, im_w, im_h, delta_crop):
        y1 = max(0, point.y1 - delta_crop)
        y2 = min(im_h, point.y2 + delta_crop)
        x1 = max(0, point.x1 - delta_crop)
        x2 = min(im_w, point.x2 + delta_crop)

        return x1, y1, x2, y2

    def _find_max_contour(self, contours):
        # max contour
        max_contour = None
        max_area = -1
        for i in range(len(contours)):
            area = cv2.contourArea(contours[i])
            if area > max_area:
                max_contour = contours[i]
                max_area = area
        return max_contour, max_area

    def run(self):
        ir_image, boxes = self._load_annotations()
        im = self._read_ir_norm(os.path.join(ir_image.directory, ir_image.filename))

        output_dict = {}

        im_w, im_h = im.shape[1], im.shape[0]
        good_boxes = []
        duplicate_boxes = []
        for b in boxes:
            found = False
            for b2 in good_boxes:
                if b.x1 == b2.x1 and b.x2 == b2.x2 and b.y1 == b2.y1 and b.y2 == b2.y2:
                    found = True
                    break
            if found:
                duplicate_boxes.append(b)
            else:
                good_boxes.append(b)
        output_dict['duplicates'] = {}

        to_add = []
        for b in duplicate_boxes:
            for b2 in good_boxes:
                if b.x1 == b2.x1 and b.x2 == b2.x2 and b.y1 == b2.y1 and b.y2 == b2.y2:
                    to_add.append(b2)
        duplicate_boxes += to_add

        for b in duplicate_boxes:
            output_dict['duplicates'][b.id] = b.to_dict()
        boxes = good_boxes
        for current_box in boxes:
            output_dict[current_box.id] = {'db_box': current_box.to_dict()}
            crop = self._get_crop(current_box, im_w, im_h, self.delta_crop)
            crop_x1, crop_y1, crop_x2, crop_y2 = crop

            # find other points that lie within this crop and black them out
            # this is important for images with high density of hotspots
            black_out_regions = []
            for pt in boxes:
                # iterate through all points except for the current one and find ones that intersect with our croppeed region
                if pt.id == current_box.id:
                    continue
                intersected_region = self._intersection(crop, pt)
                if not intersected_region:
                    continue
                (inter_x1, inter_y1), (inter_x2, inter_y2) = intersected_region
                inter_x1 -= crop_x1
                inter_x2 -= crop_x1
                inter_y1 -= crop_y1
                inter_y2 -= crop_y1
                black_out_regions.append(((inter_x1, inter_y1), (inter_x2, inter_y2)))
            # 'black out' the detected regions where other hotspots are in the crop using the mean pixel value of the crop
            crop = im[crop_y1:crop_y2, crop_x1:crop_x2, ].copy()
            mean_pixel_value = crop.mean()
            for ((x1, y1), (x2, y2)) in black_out_regions:
                crop[y1:y2, x1:x2, ] = mean_pixel_value

            # Now we have the cropped region with nearby hotspots removed
            thresh = int(np.nanpercentile(crop[crop > 0.0], self.percentile))
            mask = crop > thresh

            contours, hierarchy = cv2.findContours(mask.astype(np.uint8), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

            max_contour, max_area = self._find_max_contour(contours)

            if max_contour is not None:
                # reset contour position in crop relative to entire image
                max_contour[:, :, 0] += crop_x1
                max_contour[:, :, 1] += crop_y1

                contour = cv2.convexHull(max_contour)
                x, y, w, h = cv2.boundingRect(contour)
                x1_new = x
                y1_new = y
                x2_new = x + w
                y2_new = y + h

                output_dict[current_box.id]['new_box'] = {'x1': x1_new, 'y1': y1_new, 'x2': x2_new, 'y2': y2_new,
                                                          'area': w * h}
                new_box = output_dict[current_box.id]['new_box']
                old_box = output_dict[current_box.id]['db_box']
                new_x_cent = int(new_box['x1'] + (new_box['x2'] - new_box['x1']) / 2)
                new_y_cent = int(new_box['y1'] + (new_box['y2'] - new_box['y1']) / 2)
                old_x_cent = int(old_box['x1'] + (old_box['x2'] - old_box['x1']) / 2)
                old_y_cent = int(old_box['y1'] + (old_box['y2'] - old_box['y1']) / 2)

                dist = math.hypot(old_x_cent - new_x_cent, old_y_cent - new_y_cent)
                output_dict[current_box.id]['stats'] = {'distance': dist}
            else:
                output_dict[current_box.id]['new_box'] = None

        with self.output().open('w') as f:
            f.write(json.dumps(output_dict, indent=4))

        # Call draw if set parameter
        if self.draw_result:
            yield DrawBoundingBoxesTask(input_file=self.output().path)

class AllPt2BoxTask(luigi.WrapperTask):
    output_root = luigi.Parameter()
    delta_crop = luigi.IntParameter(default=10)
    percentile = luigi.IntParameter(default=90)

    intersected_point_delta = luigi.IntParameter(default=4)
    max_images = luigi.IntParameter(default=-1)
    draw_only = luigi.BoolParameter(default=False)

    def requires(self):
        s = Session()

        counts = s.query(Annotation.ir_event_key, func.count(Annotation.ir_event_key))\
            .join(Annotation.ir_box)\
            .filter(BoundingBox.is_point)\
            .group_by(Annotation.ir_event_key).all()


        counts = sorted(counts, key=lambda x: x[1], reverse=True)
        s.close()
        if self.max_images > 0:
            counts = counts[:max(self.max_images, len(counts))]


        for k, count in counts:
            draw_result = count > 4
            if self.draw_only and not draw_result:
                continue
            yield Pt2BoxTask(ir_image_key=k, output_root=self.output().path,
                             delta_crop=self.delta_crop,
                             percentile=self.percentile,
                             intersected_point_delta=self.intersected_point_delta,
                             draw_result=draw_result)

    def output(self):
        dir = '%d_%d_%d' % (self.delta_crop, self.percentile, self.intersected_point_delta)
        json_root = os.path.join(self.output_root, dir)
        # return the output directory where the json files are
        return luigi.local_target.LocalTarget(json_root)

    def complete(self):
        return False

    def run(self):
        yield StatsPt2BoxTask(input_root=self.output().path)
