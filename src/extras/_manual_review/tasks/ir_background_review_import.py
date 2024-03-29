import glob
import json
import os
from random import random

import cv2
import luigi

from extras._manual_review.tasks import IRBackgroundReviewCreateTask
from noaadb import Session
from noaadb.schema.models import *
from pipelines.ingest.util.image_utilities import file_key

class IRImportBackgroundReviewTask(luigi.Task):
    def requires(self):
        return {'manual_review': IRBackgroundReviewCreateTask(),
                'tables': CreateTableTask(children=["IRUncertainBackground", "IRVerifiedBackground"])}
    def output(self):
        return []

    def run(self):
        input = self.input()
        im_dir = input['manual_review']['image_dir']
        im_list_target = input['manual_review']['image_list']

        images_reviewed = []
        with im_list_target.open('r') as f:
            for l in f.readlines():
                images_reviewed.append(file_key(os.path.basename(l.strip())))

        types = ('*.png', '*.jpg')  # the tuple of file types
        images_kept = []
        for ext in types:
            images_kept.extend(glob.glob(os.path.join(im_dir.path, ext)))
        images_kept = [file_key(os.path.basename(x)) for x in images_kept]

        images_removed = [] # images that need review are not longer present in the folder
        for x in images_reviewed:
            if x not in images_kept:
                images_removed.append(x)

        s = Session()
        with_errors = 0
        without_errors = 0
        for im_key in images_removed:
            exists = s.query(IRUncertainBackground).filter(IRUncertainBackground.ir_event_key == im_key).first()
            if exists is None: # if not exists add
                obj = IRUncertainBackground(ir_event_key=im_key)
                obj.register()
                with_errors += 1
                s.add(obj)

        for im_key in images_kept:
            exists = s.query(IRVerifiedBackground).filter(IRVerifiedBackground.ir_event_key == im_key).first()
            if exists is None: # if not exists add
                obj = IRVerifiedBackground(ir_event_key=im_key)
                obj.register()
                without_errors += 1
                s.add(obj)
        s.commit()
        s.close()


if __name__ == '__main__':
    luigi_project_config = '../create_manual_review.cfg'
    luigi.configuration.add_config_path(luigi_project_config)
    luigi.build([IRImportBackgroundReviewTask()], local_scheduler=True)

