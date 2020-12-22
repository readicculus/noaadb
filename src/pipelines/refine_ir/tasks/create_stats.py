import json
import os

import luigi
from luigi.local_target import LocalFileSystem
from core import ForcibleTask
import numpy as np
import matplotlib.pyplot as plt

from pipelines.refine_ir import pipelineConfig
from pipelines.refine_ir.tasks import CreateRefinementDataTask


class CreateStatsTask(ForcibleTask):
    def requires(self):
        return CreateRefinementDataTask()

    def cleanup(self):
        outputs = luigi.task.flatten(self.output())
        for out in outputs:
            if out.exists():
                out.remove()

    def output(self):
        config = pipelineConfig()
        out_dir = os.path.join(config.output_root, 'stats')
        out = {'metrics': luigi.LocalTarget(os.path.join(out_dir, 'metrics.json')),
               'area_hist': luigi.LocalTarget(os.path.join(out_dir, 'area_hist.png')),
               'dist_hist': luigi.LocalTarget(os.path.join(out_dir, 'distance_hist.png')),
               'area_change_hist': luigi.LocalTarget(os.path.join(out_dir, 'area_change_hist.png')),
               'area_change_pixels_hist': luigi.LocalTarget(os.path.join(out_dir, 'area_change_pixels_hist.png')),
               'duplicates': luigi.LocalTarget(os.path.join(out_dir, 'duplicates.json')),
               'missing': luigi.LocalTarget(os.path.join(out_dir, 'missing.json'))}
        return out

    def _group_duplicates(self, duplicates):
        groups = {}
        for a in duplicates:
            d = duplicates[a]
            k = '%d-%d-%d-%d' % (d['x1'], d['x2'], d['y1'], d['y2'])
            if k not in groups:
                groups[k] = []

            groups[k].append(d)

        res = list(groups.values())
        return groups

    def run(self):
        json_objects = []
        file_keys = []
        json_files = []
        with self.input().open('r') as f:
            for l in f.readlines():
                l = l.strip()
                json_files.append(l)
        for json_f in json_files:
            with open(json_f, 'r') as f:
                image_obj = json.loads(f.read())
                json_objects.append(image_obj)
                file_keys.append(os.path.basename(json_f).replace('.json', ''))

        all_duplicates = {}
        dup_count = 0
        all_missing = {}
        all_distances = []
        all_areas_post = []
        all_areas_change = []
        all_areas_change_pixels = []
        total_new_box_count = 0
        for image_obj, image_key in zip(json_objects, file_keys):
            image_duplicates = image_obj['duplicates']
            if len(image_duplicates) > 0:
                all_duplicates[image_key] = {}
            all_duplicates[image_key] = self._group_duplicates(image_duplicates)
            for d_k in all_duplicates[image_key]:
                dup_count += len(all_duplicates[image_key][d_k]) - 1
            for k in image_obj['unique']:
                old = image_obj['unique'][k]['pre']
                new = image_obj['unique'][k]['post']

                if new is None:
                    all_missing[k] = old
                    continue
                stats = image_obj['unique'][k]['stats']
                distance = stats['distance']
                area_change = stats['area_change']
                area_change_pixels = stats['area_change_pixels']
                all_distances.append(distance)
                all_areas_change.append(area_change)
                all_areas_change_pixels.append(area_change_pixels)
                all_areas_post.append(new['area'])
                total_new_box_count += 1

        output = self.output()

        with output['duplicates'].open('w') as f:
            f.write(json.dumps(all_duplicates, indent=4))

        with output['missing'].open('w') as f:
            f.write(json.dumps(all_missing, indent=4))

        # Create metrics.json
        all_distances = np.array(all_distances)
        all_areas_post = np.array(all_areas_post)
        all_areas_change = np.array(all_areas_change)
        all_areas_change_pixels = np.array(all_areas_change_pixels)
        result = {'num_duplicates': dup_count,
                  'num_missing': len(all_missing),
                  'num_new_box_count': total_new_box_count,
                  'area': {'mean': float(all_areas_post.mean()), 'std': float(all_areas_post.std()),
                           'min': int(all_areas_post.min()), 'max': int(all_areas_post.max())},
                  'distance': {'mean': float(all_distances.mean()), 'std': float(all_distances.std()),
                               'min': int(all_distances.min()), 'max': int(all_distances.max())},
                  'area_change_magnitude': {'mean': float(all_areas_change.mean()),
                                                   'std': float(all_areas_change.std()),
                                                   'min': int(all_areas_change.min()),
                                                   'max': int(all_areas_change.max())},
                  'area_change_pixels': {'mean': float(all_areas_change_pixels.mean()),
                                                   'std': float(all_areas_change_pixels.std()),
                                                   'min': int(all_areas_change_pixels.min()),
                                                   'max': int(all_areas_change_pixels.max())},
                  }
        with output['metrics'].open('w') as f:
            f.write(json.dumps(result, indent=4))

        # Plot the histograms
        plt.figure()
        plt.hist(all_areas_post, bins=100)
        plt.savefig(output['area_hist'].path)
        plt.figure()
        plt.hist(all_distances, bins=100)
        plt.savefig(output['dist_hist'].path)
        plt.figure()
        plt.hist(all_areas_change, bins=100, range=[0,10])
        plt.savefig(output['area_change_hist'].path)
        plt.figure()
        plt.hist(all_areas_change_pixels, bins=100)
        plt.savefig(output['area_change_pixels_hist'].path)

