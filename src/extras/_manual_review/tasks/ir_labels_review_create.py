import json
import os
from random import random

import cv2
import luigi
from sqlalchemy import exists

from core import ForcibleTask
from noaadb import Session
from noaadb.schema.models import *
from pipelines.ingest.util.image_utilities import file_key

class IRLabelsReviewCreateTask(luigi.Task):
    data_root = luigi.Parameter()
    job_name = luigi.Parameter()
    def output(self):
        return {'image_list': luigi.LocalTarget(os.path.join(str(self.data_root), str(self.job_name), 'exported_images.txt')),
                'image_dir': luigi.LocalTarget(os.path.join(str(self.data_root), str(self.job_name)))}

    def run(self):
        s = Session()
        # all_valid_images = s.query(IRImage)\
        #     .join(TrainTestValid, TrainTestValid.ir_event_key == IRImage.event_key)\
        #     .filter(TrainTestValid.type == TrainTestValidEnum.valid).all()
        all_train_images = s.query(IRImage)\
            .join(TrainTestValid, TrainTestValid.ir_event_key == IRImage.event_key)\
            .filter(TrainTestValid.type == TrainTestValidEnum.test).all()
        images = all_train_images

        ir_event_keys = [x.event_key for x in images]
        all_annots = s.query(Annotation).filter(Annotation.ir_event_key.in_(ir_event_keys)).join(Annotation.ir_box).all()
        im_label_dict = {}
        for im in images:
            im_label_dict[im.event_key] = {'im': im, 'labels': []}
        for annot in all_annots:
            im_label_dict[annot.ir_event_key]['labels'].append(annot.ir_box)

        s.close()
        output = self.output()
        os.makedirs(output['image_dir'].path, exist_ok=True)
        saved_images = []



        for im_key in im_label_dict:
            im_obj = im_label_dict[im_key]['im']
            labels = im_label_dict[im_key]['labels']
            im = im_obj.ocv_load_normed()

            save_jpg = len(labels) > 0
            if save_jpg:
                im = cv2.cvtColor(im, cv2.COLOR_GRAY2RGB)
                for l in labels:
                    cv2.rectangle(im, (l.x1, l.y1), (l.x2, l.y2), (0, 255, 0), thickness=1)

                fn = im_obj.filename
                fn = '.'.join(fn.split('.')[:-1]) + '.jpg'
            else:
                fn = im_obj.filename
                fn = '.'.join(fn.split('.')[:-1]) + '.png'
            out_im_path = os.path.join(output['image_dir'].path, fn)
            cv2.imwrite(out_im_path, im)
            saved_images.append(out_im_path)
        print("%d images saved"%len(saved_images))

        with output['image_list'].open('w') as f:
            for im_path in saved_images:
                f.write("%s\n"%im_path)


if __name__ == '__main__':
    luigi_project_config = '../create_manual_review.cfg'
    luigi.configuration.add_config_path(luigi_project_config)
    luigi.build([IRLabelsReviewCreateTask()], local_scheduler=True)

