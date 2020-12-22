import json
import logging
import os

import luigi
from luigi.local_target import LocalFileSystem

from core import ForcibleTask, AlwaysRunTask, SQLAlchemyCustomTarget
from noaadb import Session, DATABASE_URI
from noaadb.schema.models import Annotation, BoundingBox
from shutil import copyfile

from pipelines.refine_ir import pipelineConfig
from pipelines.refine_ir.tasks import CreateRefinementDataTask, CreateStatsTask, DrawBoundingBoxesTask


class FilterRefinementsTask(ForcibleTask):
    task_dir = luigi.Parameter(default='filtered')

    resolve_duplicates = luigi.Parameter()
    area_change_magnitude_range = luigi.ListParameter()
    area_change_pixels_range = luigi.ListParameter()
    distance_range = luigi.ListParameter()
    max_draw = luigi.IntParameter()


    def requires(self):
        yield CreateRefinementDataTask()
        yield CreateStatsTask()

    def output(self):
        config = pipelineConfig()
        to_remove = luigi.LocalTarget(os.path.join(config.output_root,self.task_dir, 'to_remove.json'))
        to_update = luigi.LocalTarget(os.path.join(config.output_root,self.task_dir, 'to_update.json'))
        metrics = luigi.LocalTarget(os.path.join(config.output_root,self.task_dir, 'metrics.json'))
        return {'to_remove':to_remove, 'to_update': to_update , 'metrics': metrics}

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

    def _create_updates(self, all_image_refinements):
        to_update = {}
        to_update_ct = 0
        filters = ['distance', 'area_change', 'area_change_pixels']
        filters_range = {
                            'distance':self.distance_range,
                            'area_change': self.area_change_magnitude_range,
                            'area_change_pixels': self.area_change_pixels_range}

        filtered_ct = {k:0 for k in filters}
        filtered_ct['no_refined_box_found'] = 0
        for image_key in all_image_refinements:
            to_update[image_key] = {}
            for ir_box_key in all_image_refinements[image_key]['unique']:
                box = all_image_refinements[image_key]['unique'][ir_box_key]
                stats = box['stats']
                if stats is None:
                    filtered_ct['no_refined_box_found'] += 1
                    continue
                filter_this = False
                for f in filters:
                    f_range = filters_range[f]
                    if stats[f] < f_range[0] or stats[f] > f_range[1]:
                        filter_this = True
                        filtered_ct[f] += 1
                        break
                if not filter_this:
                    to_update[image_key][ir_box_key] = all_image_refinements[image_key]['unique'][ir_box_key]
                    to_update_ct += 1
        return to_update, filtered_ct, to_update_ct


    def run(self):
        config = pipelineConfig()
        os.makedirs(os.path.join(config.output_root,self.task_dir), exist_ok=True)
        json_files = []
        with self.input()[0].open('r') as f:
            for l in f.readlines():
                l = l.strip()
                json_files.append(l)
        all_image_refinements = {}
        for file in json_files:
            with open(file, 'r') as f:
                image_obj = json.loads(f.read())
                file_key = os.path.basename(file).replace('.json', '')
                all_image_refinements[file_key] = image_obj
        if self.resolve_duplicates == 'ignore':
            with self.output()['to_remove'].open('w') as f:
                f.write("{}")


        to_update, filtered_cts, to_update_ct = self._create_updates(all_image_refinements)

        with self.output()['to_update'].open('w') as f:
            f.write(json.dumps(to_update, indent=4))

        metrics = {'filtered': filtered_cts, 'to_update_ct': to_update_ct}
        with self.output()['metrics'].open('w') as f:
            f.write(json.dumps(metrics, indent=4))


   # for image_key in json_objects['unique']:
        #     to_update[image_key] = {}
        #     if len(json_objects[image_key]['duplicates']) > 0:
        #         ir_box_ids = list(json_objects[image_key]['duplicates'].keys())
        #         annotations_with_ir_box = s.query(Annotation).filter(Annotation.ir_event_key == image_key).filter(Annotation.ir_box_id.in_(ir_box_ids)).all()
        #         duplicate_groups=self._group_duplicates(annotations_with_ir_box)
        #         for dup_group in duplicate_groups:
        #             annotations_with_ir_box.sort(key=lambda x: x.hotspot_id)
        #             original = dup_group[0]
        #             to_remove = dup_group[1:]
        #             for b in dup_group:
        #                 box_id = str(b.ir_box_id)
        #                 if box_id in json_objects[image_key]:
        #                     temp = json_objects[image_key][box_id]
        #                     del json_objects[image_key][box_id]
        #                     temp['db_box']['id'] = original.ir_box_id
        #                     json_objects[image_key][str(original.ir_box_id)] = temp
        #
        #             for an in to_remove:
        #                 ir_box_id = str(an.ir_box_id)
        #                 to_update[image_key][ir_box_id] = {"REMOVE": json_objects[image_key]['duplicates'][ir_box_id]}
@FilterRefinementsTask.event_handler(luigi.Event.SUCCESS)
def celebrate_success(task):
    tasks = []
    max_draw = task.max_draw
    to_draw = 0
    with task.output()['to_update'].open('r') as f:
        to_update = json.loads(f.read())
        for image_key in to_update:
            to_draw+=1
            if to_draw >= max_draw:
                break
            tasks.append(DrawBoundingBoxesTask(image_key=image_key, refinement_dict=json.dumps(to_update[image_key])))
    luigi.build(tasks, detailed_summary=False, local_scheduler=True)
