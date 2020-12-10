import json
import os

import luigi
from luigi.local_target import LocalFileSystem
from core import ForcibleTask
import numpy as np
import matplotlib.pyplot as plt

class StatsPt2BoxTask(ForcibleTask):
    input_root = luigi.Parameter()


    def output(self):
        base, dir = os.path.split(self.input_root)
        out_dir = os.path.join(base, '%s_stats' % dir)
        out = {'info': luigi.LocalTarget(os.path.join(out_dir, 'info.json')),
               'area_hist': luigi.LocalTarget(os.path.join(out_dir, 'area_hist.png')),
               'dist_hist': luigi.LocalTarget(os.path.join(out_dir, 'distance_hist.png')),
               'duplicates': luigi.LocalTarget(os.path.join(out_dir, 'duplicates.json')),
               'missing': luigi.LocalTarget(os.path.join(out_dir, 'missing.json'))}
        return out

    def cleanup(self):
        outputs = luigi.task.flatten(self.output())
        for out in outputs:
            if out.exists():
                out.remove()

    def _group_duplicates(self, duplicates):
        groups = {}
        for a in duplicates:
            d = duplicates[a]
            k = '%d-%d-%d-%d' % (d['x1'],d['x2'],d['y1'],d['y2'])
            if k not in groups:
                groups[k] = []

            groups[k].append(d)

        res = list(groups.values())
        return groups
    def run(self):
        fs = LocalFileSystem()
        files = fs.listdir(self.input_root)
        json_objects = []
        file_keys = []
        for file in files:
            with open(file, 'r') as f:
                image_obj = json.loads(f.read())
                json_objects.append(image_obj)
                file_keys.append(os.path.basename(file).replace('.json', ''))

        all_duplicates = {}
        dup_count = 0
        all_missing = {}
        all_distances = []
        all_areas = []
        total_new_box_count = 0
        for image_obj, image_key in zip(json_objects, file_keys):
            image_duplicates = image_obj['duplicates']
            if len(image_duplicates) > 0:
                all_duplicates[image_key] = {}
            all_duplicates[image_key] = self._group_duplicates(image_duplicates)
            for d_k in all_duplicates[image_key]:
                dup_count += len(all_duplicates[image_key][d_k])-1
            for k in image_obj:
                if k == 'duplicates':
                    continue

                old = image_obj[k]['db_box']
                new = image_obj[k]['new_box']
                stats = image_obj[k]['stats']

                if new is None:
                    all_missing[k] = old
                    continue

                distance = stats['distance']
                all_distances.append(distance)
                all_areas.append(new['area'])
                total_new_box_count += 1
        all_distances = np.array(all_distances)
        all_areas = np.array(all_areas)
        output = self.output()

        with output['duplicates'].open('w') as f:
            f.write(json.dumps(all_duplicates, indent=4))

        with output['missing'].open('w') as f:
            f.write(json.dumps(all_missing, indent=4))

        result = {'num_duplicates': dup_count,
                  'num_missing': len(all_missing),
                  'num_new_box_count': total_new_box_count,
                  'area': {'mean': float(all_areas.mean()), 'std': float(all_areas.std()), 'min': int(all_areas.min()), 'max':int(all_areas.max())},
                  'distance':  {'mean': float(all_distances.mean()), 'std': float(all_distances.std()), 'min': int(all_distances.min()), 'max':int(all_distances.max())},
                  }
        with output['info'].open('w') as f:
            f.write(json.dumps(result, indent=4))
        plt.figure()
        plt.hist(all_areas)
        plt.savefig(output['area_hist'].path)
        plt.figure()
        plt.hist(all_distances)
        plt.savefig(output['dist_hist'].path)
        x=1
