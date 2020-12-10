import copy
import json
import logging
import math
import os
import luigi
from sqlalchemy import tuple_, or_

from core import ForcibleTask, SQLAlchemyCustomTarget, AlwaysRunTask
from pipelines.ingest.tasks import IngestAllTask,CreateTableTask
from noaadb import Session, DATABASE_URI, engine
from noaadb.schema.models import *
from collections import defaultdict
import pandas as pd

class SpeciesCountsTask(ForcibleTask):
    output_root = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(str(self.output_root), 'species_counts.json'))

    def requires(self):
        yield IngestAllTask()

    def cleanup(self):
        if self.output().exists():
            self.output().remove()

    def run(self):
        s = Session()
        annotations = s.query(Annotation).all()
        species = s.query(Species).all()
        initial_dict = {x.name: 0 for x in species}

        species_counts_by_image = {}
        for annotation in annotations:
            key = "%s" % annotation.eo_event_key
            if not key in species_counts_by_image:
                species_counts_by_image[key] = initial_dict.copy()
            species_counts_by_image[key][annotation.species.name] += 1
        s.close()
        with self.output().open('w') as f:
            f.write(json.dumps(species_counts_by_image))


class PartitionAnnotationsTask(ForcibleTask):
    output_root = luigi.Parameter()
    num_partitions = luigi.IntParameter()

    def output(self):
        outputs = {}
        for i in range(self.num_partitions):
            outputs[i] = luigi.LocalTarget(os.path.join(str(self.output_root), 'partition_%d.json' % i))

        outputs['distribution'] = luigi.LocalTarget(os.path.join(str(self.output_root), 'distribution.json'))
        outputs['counts'] = luigi.LocalTarget(os.path.join(str(self.output_root), 'counts.json'))
        return outputs


    def requires(self):
        return SpeciesCountsTask()##output_root=os.path.join(self.output_root,'SpeciesCountsTask')

    def cleanup(self):
        for target in self.output().values():
            if target.exists():
                target.remove()

    def _recalc_metrics(self, counts, idx, total_counts, b_counts, b_metrics):
        bucket_counts = copy.deepcopy(b_counts)
        bucket_metrics = copy.deepcopy(b_metrics)

        # append counts to bucket at idx
        for k in bucket_counts[idx]:
            bucket_counts[idx][k] += counts[k]

        # recalculate metrics
        for b_idx in bucket_counts:
            b_count = bucket_counts[b_idx]
            for k in b_count:
                bucket_metrics[b_idx][k] = 0 if total_counts[k] == 0 else b_count[k] / total_counts[k]
        return bucket_metrics

    def _metrics_difference(self, metrics):
        # calculate difference between species distribution
        species = list(metrics.values())[0].keys()
        sp_diff = {}
        for sp in species:
            sp_metrics = []
            for b_idx in metrics:
                b_metric = metrics[b_idx]
                sp_metrics.append(b_metric[sp])
            ma = max(sp_metrics)
            mi = min(sp_metrics)
            sp_diff[sp] = abs(ma-mi)
        return sp_diff

    def run(self):
        logger = logging.getLogger('luigi-interface')

        with self.input().open('r') as f:
            species_counts_by_image = json.loads(f.read())

        keys = list(species_counts_by_image.values())[0].keys()
        init = {k: 0 for k in keys}
        # each bucket is a partition, each partition is an array of image names
        # metrics contains the rolling distribution per species
        buckets = {i: [] for i in range(self.num_partitions)}
        bucket_counts = {i: init.copy() for i in range(self.num_partitions)}

        bucket_metrics = {i: init.copy() for i in range(self.num_partitions)}
        total_counts = init.copy()

        total = len(species_counts_by_image)
        for i, k in enumerate(list(species_counts_by_image.keys())):
            if i % 100 == 0:
                self.set_status_message("Progress: %d / %d" % (i, total))
                self.set_progress_percentage(i / total * 100)
                logger.info("Progress: %d / %d" % (i, total))
            counts = species_counts_by_image[k]

            for k_c in counts:
                total_counts[k_c] += counts[k_c]

            mets_by_idx = {}
            for idx in buckets:
                mets_by_idx[idx] = self._recalc_metrics(counts, idx, total_counts, bucket_counts, bucket_metrics)
            diff_by_idx = {}
            for idx in mets_by_idx:
                diff_by_idx[idx] = self._metrics_difference(mets_by_idx[idx])

            best_idx = None
            best_score = math.inf
            important = ['Ringed Seal', 'Bearded Seal', 'Polar Bear', 'UNK Seal']
            for idx in diff_by_idx:
                score = sum([diff_by_idx[idx][x] for x in important])
                if score < best_score:
                    best_score = score
                    best_idx = idx

            # append counts to bucket at idx
            for k_s in bucket_counts[best_idx]:
                bucket_counts[best_idx][k_s] += counts[k_s]

            buckets[best_idx].append(k)

            # recalculate metrics
            for b_idx in bucket_counts:
                b_count = bucket_counts[b_idx]
                for k_s in b_count:
                    bucket_metrics[b_idx][k_s] = 0 if total_counts[k_s] == 0 else b_count[k_s] / total_counts[k_s]


        output = self.output()
        for b_idx in buckets:
            with output[b_idx].open('w') as f:
                f.write(json.dumps(buckets[b_idx]))

        with output['counts'].open('w') as f:
                f.write(json.dumps(bucket_counts, indent=4, sort_keys=True))


        with output['distribution'].open('w') as f:
                f.write(json.dumps(bucket_metrics, indent=4, sort_keys=True))


class MakeTrainTestValidTask(ForcibleTask):
    output_root = luigi.Parameter()
    train_partitions = luigi.ListParameter()
    test_partitions = luigi.ListParameter()
    valid_partitions = luigi.ListParameter()

    def requires(self):
        # partition_annotations_root = os.path.join(self.output_root,'PartitionAnnotationsTask')
        return PartitionAnnotationsTask()

    def output(self):
        out = {
            'test_images': luigi.LocalTarget(os.path.join(str(self.output_root), 'test.json')),
            'train_images': luigi.LocalTarget(os.path.join(str(self.output_root), 'train.json')),
            'valid_images': luigi.LocalTarget(os.path.join(str(self.output_root), 'valid.json'))
        }
        return out

    def cleanup(self):
        for target in self.output().values():
            if target.exists():
                target.remove()

    def run(self):
        keys = list(self.input().keys())
        num_partitions = 0
        partitions = {}

        for k in keys:
            if isinstance(k, int):
                num_partitions+=1
                with self.input()[k].open('r') as f:
                    partitions[k] = json.loads(f.read())

        all_idxs = self.train_partitions + self.test_partitions + self.valid_partitions
        if len(self.train_partitions + self.test_partitions + self.valid_partitions) != num_partitions:
            raise Exception('len(self.train_partitions + self.test_partitions + self.valid_partitions) != num_partitions')
        if any(all_idxs.count(x) > 1 for x in all_idxs):
            raise Exception('Duplicate idxs in %s' % all_idxs)

        test_images = []
        for idx in self.test_partitions:
            test_images += partitions[idx]

        train_images = []
        for idx in self.train_partitions:
            train_images += partitions[idx]

        valid_images = []
        for idx in self.valid_partitions:
            valid_images += partitions[idx]

        output = self.output()
        with output['test_images'].open('w') as f:
            f.write(json.dumps(test_images))

        with output['train_images'].open('w') as f:
            f.write(json.dumps(train_images))


        with output['valid_images'].open('w') as f:
            f.write(json.dumps(valid_images))


# class CheckOneIRToEOTask(AlwaysRunTask):
#     def run(self):
#         s = Session()
#
#         eo_ir_pairs = s.query(Annotation.eo_event_key, Annotation.ir_event_key) \
#             .filter(Annotation.eo_event_key != Annotation.ir_event_key) \
#             .distinct(tuple_(Annotation.eo_event_key, Annotation.ir_event_key)).all()
#         ir_keys = []
#         eo_keys = []
#         for eo_k, ir_k in eo_ir_pairs:
#             eo_keys.append(eo_k)
#             ir_keys.append(ir_k)
#
#
#         duplicate_eo = set([x for x in eo_keys if eo_keys.count(x) > 1])
#         duplicate_ir = set([x for x in ir_keys if ir_keys.count(x) > 1])
#         s.close()
#
#         if len(duplicate_eo) > 0 or len(duplicate_ir) > 0:
#             raise NOAADBDataIntegrityException('IR Keys associated with more than one eo image exist.')

class TrainTestValidStatsTask(AlwaysRunTask):
    def get_species_total_count_in_db(self, s, species_name):
        return s.query(Annotation).join(Species).filter(Species.name == species_name).count()

    def get_counts(self, s, type):
        eo_ir_pairs = s.query(TrainTestValid.eo_event_key, TrainTestValid.ir_event_key) \
            .filter(TrainTestValid.type == type).all()

        ir_keys = []
        eo_keys = []
        for eo_k, ir_k in eo_ir_pairs:
            eo_keys.append(eo_k)
            ir_keys.append(ir_k)

        annotations = s.query(Annotation) \
            .filter(or_(Annotation.eo_event_key.in_(eo_keys), Annotation.ir_event_key.in_(ir_keys))).all()

        species = s.query(Species).all()

        counts = {x.name: 0 for x in species}
        for annotation in annotations:
            counts[annotation.species.name] += 1

        return counts

    def run(self):
        s = Session()
        train_counts = self.get_counts(s, TrainTestValidEnum.train)
        test_counts = self.get_counts(s, TrainTestValidEnum.test)
        valid_counts = self.get_counts(s, TrainTestValidEnum.valid)

        total = defaultdict(int)
        for elm in [train_counts, test_counts, valid_counts]:
            for k, v in elm.items():
                total[k] += v

        total_in_db = defaultdict(int)
        for k,v in total.items():
            total_in_db[k] = self.get_species_total_count_in_db(s, k)

        output = pd.DataFrame({'train': train_counts,
                               'test': test_counts,
                               'valid': valid_counts,
                               'total': total,
                               'total_in_db': total_in_db})
        s.close()
        output.to_sql(name='train_test_valid_stats',
                      schema='annotation_data',
                      index_label='Species',
                      con=engine,
                      if_exists='replace')


class LoadTrainTestValidTask(ForcibleTask):

    def requires(self):
        yield MakeTrainTestValidTask()
        yield CreateTableTask(children=["TrainTestValid"])
        # yield CheckOneIRToEOTask()


    def output(self):
        return SQLAlchemyCustomTarget(DATABASE_URI, 'LoadTrainTestValidTask', 'LoadTrainTestValidTask',
                                     echo=False)

    def cleanup(self):
        self.output().remove()
        s = Session()
        s.query(TrainTestValid).delete()
        s.commit()
        s.close()

    def load_image_list(self, target, type):
        with target.open('r') as f:
            images = json.loads(f.read())

        s = Session()
        eo_ir_pairs = s.query(Annotation.eo_event_key, Annotation.ir_event_key) \
            .filter(Annotation.eo_event_key.in_(images)) \
            .distinct(tuple_(Annotation.eo_event_key, Annotation.ir_event_key)).all()

        for eo_k, ir_k in eo_ir_pairs:
            ttv_obj = TrainTestValid(eo_event_key=eo_k, ir_event_key=ir_k, type=type)
            s.add(ttv_obj)
        s.commit()
        s.close()


    def run(self):
        image_lists_target = self.input()[0]
        train = image_lists_target['train_images']
        test = image_lists_target['test_images']
        valid = image_lists_target['valid_images']

        self.load_image_list(train, 'train')
        self.load_image_list(test, 'test')
        self.load_image_list(valid, 'valid')
        yield TrainTestValidStatsTask()
        self.output().touch()



