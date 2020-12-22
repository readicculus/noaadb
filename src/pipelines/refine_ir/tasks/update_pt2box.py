import json
import logging
import os
from shutil import copyfile

import luigi
from luigi.local_target import LocalFileSystem

from core import ForcibleTask, AlwaysRunTask, SQLAlchemyCustomTarget
from noaadb import Session, DATABASE_URI
from noaadb.schema.models import Annotation, BoundingBox
from pipelines.refine_ir import pipelineConfig
from pipelines.refine_ir.tasks import FilterRefinementsTask


class ValidateUpdateTask(AlwaysRunTask):
    def requires(self):
        if not FilterRefinementsTask().complete():
            raise Exception('Must have completed and reviewed FilterRefinementsTask results first.')
        return FilterRefinementsTask(lock=True, force=False)

    def output(self):
       return self.input()

    def run(self):
        logger = logging.getLogger('luigi-interface')
        with self.input()['to_update'].open('r') as f:
            to_update = json.loads(f.read())
        with self.input()['to_remove'].open('r') as f:
            to_remove = json.loads(f.read())
        s = Session()
        removed_ct = 0
        # check the removed boxes
        # TODO: implement this

        # check the updated boxes
        modified_ct = 0
        for image_key in to_update:
            im_boxes = to_update[image_key]
            box_ids = list(im_boxes.keys())
            db_boxes = s.query(BoundingBox).filter(BoundingBox.id.in_(box_ids)).all()
            assert(len(box_ids) == len(db_boxes))
            for box in db_boxes:
                id = str(box.id)
                modified_ct += 1
                pre = im_boxes[id]['pre']
                assert(pre['x1'] == box.x1)
                assert (pre['x2'] == box.x2)
                assert (pre['y1'] == box.y1)
                assert (pre['y2'] == box.y2)
        s.close()
        logger.info("="*50)
        logger.info("VERIFIED COMMIT PLAN")
        logger.info("%d IR BOXES TO BE REMOVED" % removed_ct)
        logger.info("%d IR BOXES TO BE MODIFIED" % modified_ct)
        logger.info("="*50)

class CommitRefinementsTask(luigi.Task):
    commit_id = luigi.Parameter()
    task_dir = luigi.Parameter('commit')
    def requires(self):
        return ValidateUpdateTask()

    def output(self):
        config = pipelineConfig()
        os.makedirs(os.path.join(config.output_root, str(self.task_dir)), exist_ok=True)
        return {'sqla_marker': SQLAlchemyCustomTarget(DATABASE_URI, 'refine_ir', str(self.commit_id), echo=False),
                'to_update': luigi.LocalTarget(os.path.join(config.output_root, str(self.task_dir), '%s_to_update.json'%str(self.commit_id))),
                'to_remove': luigi.LocalTarget(os.path.join(config.output_root, str(self.task_dir), '%s_to_remove.json'%str(self.commit_id)))
                }

    def _remove_box(self, s, box_id):
        annotation_obj = s.query(Annotation).filter(Annotation.ir_box_id == box_id).one()
        annotation_obj.ir_box_id = None
        s.flush()
        s.query(BoundingBox).filter(BoundingBox.id == box_id).delete()
        s.flush()
        check1 = s.query(Annotation).filter(Annotation.ir_box_id == box_id).all()
        check2 = s.query(Annotation).filter(Annotation.eo_box_id == annotation_obj.eo_box_id).all()
        assert (len(check1) == 0)
        assert (len(check2) > 0)

    def _update_box(self, s, box):
        box_id = box['pre']['id']
        box_obj = s.query(BoundingBox).filter(BoundingBox.id == box_id).one()
        new_box = box['post']
        box_obj.x1 = new_box['x1']
        box_obj.x2 = new_box['x2']
        box_obj.y1 = new_box['y1']
        box_obj.y2 = new_box['y2']

    def run(self):
        # Double check before running as this will modify the database boxes
        check1 = input("Type 'YES' to commit these changes: ")
        if check1 != "YES":
            return None
        else:
            check2 = input("Double checking, type 'YES' to commit these changes, THIS IS FINAL: ")
            if check2 != "YES":
                return None
        logger = logging.getLogger('luigi-interface')

        copyfile(self.input()['to_update'].path, self.output()['to_update'].path)
        copyfile(self.input()['to_remove'].path, self.output()['to_remove'].path)

        with self.input()['to_update'].open('r') as f:
            to_update = json.loads(f.read())
        with self.input()['to_remove'].open('r') as f:
            to_remove = json.loads(f.read())
        s = Session()
        total_annotations_start = s.query(Annotation).count()
        total_boxes_start = s.query(BoundingBox).count()
        removed_ct = 0
        modified_ct = 0

        for image_key in to_remove:
            remove_from_image = to_remove[image_key]
            for box_id in remove_from_image:
                self._remove_box(s, box_id)
                removed_ct += 1

        for image_key in to_update:
            updates_for_image = to_update[image_key]
            for box_id in updates_for_image:
                box = updates_for_image[box_id]
                self._update_box(s, box)
                modified_ct+=1
        s.flush()
        total_annotations_end = s.query(Annotation).count()
        assert (total_annotations_start == total_annotations_end) # No annotations should be removed
        total_boxes_end = s.query(BoundingBox).count()
        assert (total_boxes_start-removed_ct == total_boxes_end) # No annotations should be removed
        s.commit()
        s.close()
        logger.info("="*50)
        logger.info("COMMIT COMPLETE")
        logger.info("%d IR BOXES REMOVED" % removed_ct)
        logger.info("%d IR BOXES MODIFIED" % modified_ct)
        logger.info("="*50)
        self.output()['sqla_marker'].touch()
