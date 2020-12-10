import json
import logging
import os

import luigi
from luigi.local_target import LocalFileSystem

from core import ForcibleTask, AlwaysRunTask, SQLAlchemyCustomTarget
from noaadb import Session, DATABASE_URI
from noaadb.schema.models import Annotation, BoundingBox
from shutil import copyfile

class CreateUpdatePlanTask(ForcibleTask):
    input_root = luigi.Parameter()
    output_root = luigi.Parameter()

    def output(self):
       return luigi.LocalTarget(os.path.join(str(self.output_root), 'commit_plan.json'))

    def cleanup(self):
        outputs = luigi.task.flatten(self.output())
        for out in outputs:
            if out.exists():
                out.remove()

    def _group_duplicates(self, duplicates):
        groups = {}
        for a in duplicates:
            d = a.ir_box
            k = '%d-%d-%d-%d' % (d.x1,d.y1,d.x2,d.y2)
            if k not in groups:
                groups[k] = []

            groups[k].append(a)

        res = list(groups.values())
        for r in res:
            r.sort(key=lambda x: x.hotspot_id)

        return res

    def run(self):
        fs = LocalFileSystem()
        files = fs.listdir(str(self.input_root))
        json_objects = {}
        for file in files:
            with open(file, 'r') as f:
                image_obj = json.loads(f.read())
                file_key = os.path.basename(file).replace('.json', '')
                json_objects[file_key] = image_obj

        commit_plan = {}

        s = Session()
        for image_key in json_objects:
            commit_plan[image_key] = {}
            if len(json_objects[image_key]['duplicates']) > 0:
                ir_box_ids = list(json_objects[image_key]['duplicates'].keys())
                annotations_with_ir_box = s.query(Annotation).filter(Annotation.ir_event_key == image_key).filter(Annotation.ir_box_id.in_(ir_box_ids)).all()
                duplicate_groups=self._group_duplicates(annotations_with_ir_box)
                for dup_group in duplicate_groups:
                    annotations_with_ir_box.sort(key=lambda x: x.hotspot_id)
                    original = dup_group[0]
                    to_remove = dup_group[1:]
                    for b in dup_group:
                        box_id = str(b.ir_box_id)
                        if box_id in json_objects[image_key]:
                            temp = json_objects[image_key][box_id]
                            del json_objects[image_key][box_id]
                            temp['db_box']['id'] = original.ir_box_id
                            json_objects[image_key][str(original.ir_box_id)] = temp

                    for an in to_remove:
                        ir_box_id = str(an.ir_box_id)
                        commit_plan[image_key][ir_box_id] = {"REMOVE": json_objects[image_key]['duplicates'][ir_box_id]}

            for ir_box_key in json_objects[image_key]:
                if ir_box_key == 'duplicates':
                    continue

                old = json_objects[image_key][ir_box_key]['db_box']
                new = json_objects[image_key][ir_box_key]['new_box']
                commit_plan[image_key][ir_box_key] = {'pre': old, 'post': new}


        with self.output().open('w') as f:
            f.write(json.dumps(commit_plan, indent=4))

        s.close()

class ValidateUpdateTask(AlwaysRunTask):
    input_root = luigi.Parameter()
    output_root = luigi.Parameter()

    def requires(self):
        return CreateUpdatePlanTask(input_root=self.input_root, output_root=self.output_root)

    def output(self):
       return self.input()

    def run(self):
        logger = logging.getLogger('luigi-interface')
        with self.input().open('r') as f:
            commit_plan = json.loads(f.read())

        s = Session()
        removed_ct = 0
        modified_ct = 0
        for image_key in commit_plan:
            im_boxes = commit_plan[image_key]
            box_ids = list(im_boxes.keys())
            db_boxes = s.query(BoundingBox).filter(BoundingBox.id.in_(box_ids)).all()
            assert(len(box_ids) == len(db_boxes))
            for box in db_boxes:
                id = str(box.id)
                if 'REMOVE' in im_boxes[id]:
                    removed_ct += 1
                    continue
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

class CommitUpdateTask(luigi.Task):
    input_root = luigi.Parameter()
    commit_id = luigi.Parameter()

    def requires(self):
        return ValidateUpdateTask(input_root=self.input_root)

    def output(self):
       return SQLAlchemyCustomTarget(DATABASE_URI, 'pt2box', self.commit_id, echo=False)

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
        logger = logging.getLogger('luigi-interface')
        file = self.input().path
        base = os.path.split(file)[0]
        commit_file = os.path.join(base, 'commit_%s.json'%self.commit_id)
        if not os.path.exists(commit_file):
            copyfile(file, commit_file)

        with self.input().open('r') as f:
            commit_plan = json.loads(f.read())
        s = Session()
        total_annotations_start = s.query(Annotation).count()
        total_boxes_start = s.query(BoundingBox).count()
        removed_ct = 0
        modified_ct = 0
        for image_key in commit_plan:
            im_plan = commit_plan[image_key]
            for box_id in im_plan:
                box = im_plan[box_id]
                if "REMOVE" in box:
                    self._remove_box(s, box["REMOVE"]["id"])
                    removed_ct+=1
                else:
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
        self.output().touch()
