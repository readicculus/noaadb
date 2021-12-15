import logging
from collections import defaultdict
from operator import or_

import luigi
import pandas as pd

# update the TrainTestValid table using partitions from the partition table
# IMPORTANT: this wipes the TrainTestValid rows and re-creates them
from core import AlwaysRunTask
from noaadb import Session, engine
from noaadb.schema.models import *
from pipelines.ingest.tasks import CreateTableTask


class UpdateTrainTestValidTask(luigi.Task):
    train_partitions = luigi.ListParameter()
    test_partitions = luigi.ListParameter()
    valid_partitions = luigi.ListParameter()
    lock = luigi.BoolParameter(default=True)

    def requires(self):
        return CreateTableTask(["TrainTestValid"])

    def run(self):
        logger = logging.getLogger('luigi-interface')
        if self.lock:
            logger.info("%s is locked, to run set lock=False" % self.task_id)

        s = Session()
        partition_ids = s.query(Partitions.partition).distinct().all()
        partition_ids = [x for (x,) in partition_ids]
        all_ids_cfg = self.train_partitions + self.test_partitions + self.valid_partitions

        # config error checking
        for id in all_ids_cfg:
            if id not in partition_ids:
                m = "Invalid id %s partition ids are: %s" % (str(id), str(partition_ids))
                raise Exception(m)
        if len(all_ids_cfg) != len(partition_ids):
            raise Exception('len(self.train_partitions + self.test_partitions + self.valid_partitions) != num_partitions')
        if any(all_ids_cfg.count(x) > 1 for x in all_ids_cfg):
            raise Exception('Duplicate idxs in %s' % all_ids_cfg)

        # create set for each partition
        logger.info("Deleting all records in TestTrainValid table.")
        s.query(TrainTestValid).delete()
        s.flush()
        set_type_to_partition_ids = {TrainTestValidEnum.train: self.train_partitions,
                                     TrainTestValidEnum.test: self.test_partitions,
                                     TrainTestValidEnum.valid: self.valid_partitions}
        for k in set_type_to_partition_ids:
            lst = set_type_to_partition_ids[k]
            for partition_id in lst:
                images = s.query(Partitions).filter(Partitions.partition == partition_id).all()
                for im in images:
                    ttv = TrainTestValid(eo_event_key=im.eo_event_key, ir_event_key=im.ir_event_key,
                                         type=k)
                    s.add(ttv)

        s.commit()
        s.close()


class UpdateTrainTestValidStatsTask(luigi.Task):
    def get_species_total_count_in_db(self, s, species_name):
        species_name = species_name.replace('sp_', '')
        return s.query(Annotation).join(Species).filter(Species.name == species_name).count()

    def get_counts(self, s, type):
        eo_ir_pairs = s.query(TrainTestValid.eo_event_key, TrainTestValid.ir_event_key) \
            .filter(TrainTestValid.type == type).all()

        ir_without_errors = s.query(TrainTestValid)\
            .join(IRWithoutErrors, IRWithoutErrors.ir_event_key == TrainTestValid.ir_event_key) \
            .filter(TrainTestValid.ir_event_key != None)\
            .filter(TrainTestValid.type == type).distinct(TrainTestValid.ir_event_key).all()
        ir_with_errors = s.query(TrainTestValid) \
            .join(IRWithErrors, IRWithErrors.ir_event_key == TrainTestValid.ir_event_key) \
            .filter(TrainTestValid.ir_event_key != None)\
            .filter(TrainTestValid.type == type).distinct(TrainTestValid.ir_event_key).all()

        ir_keys = []
        eo_keys = []
        for eo_k, ir_k in eo_ir_pairs:
            eo_keys.append(eo_k)
            ir_keys.append(ir_k)

        annotations = s.query(Annotation) \
            .filter(or_(Annotation.eo_event_key.in_(eo_keys), Annotation.ir_event_key.in_(ir_keys))).all()

        species = s.query(Species).all()

        counts = {'sp_'+x.name: 0 for x in species}
        annotation_eo_keys = []
        annotation_ir_keys = []
        for annotation in annotations:
            counts['sp_'+annotation.species.name] += 1
            annotation_eo_keys.append(annotation.eo_event_key)
            annotation_ir_keys.append(annotation.ir_event_key)
        annotation_eo_keys = set(annotation_eo_keys)
        annotation_ir_keys = set(annotation_ir_keys)
        eo_bg = []
        ir_bg = []
        for k in ir_keys:
            if k not in annotation_ir_keys:
                ir_bg.append(k)
        for k in eo_keys:
            if k not in annotation_eo_keys:
                eo_bg.append(k)

        counts['ir_background'] = len(ir_bg)
        counts['eo_background'] = len(eo_bg)
        counts['ir_with_animal'] = len(annotation_ir_keys)
        counts['ir_with_errors'] = len(ir_with_errors)
        counts['ir_without_errors'] = len(ir_without_errors)
        counts['eo_with_animal'] = len(annotation_eo_keys)
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
            if 'sp_' in k:
                total_in_db[k] = self.get_species_total_count_in_db(s, k)
            else:
                total_in_db[k] = None

        output = pd.DataFrame({'train': train_counts,
                               'test': test_counts,
                               'valid': valid_counts,
                               'total': total,
                               'total_in_db': total_in_db})
        s.close()
        output.to_sql(name='train_test_valid_stats',
                      schema='annotation_data',
                      index_label='Speciess',
                      con=engine,
                      if_exists='replace')

@UpdateTrainTestValidTask.event_handler(luigi.Event.SUCCESS)
def update_test_train_valid(task):
    luigi.build([UpdateTrainTestValidStatsTask()], local_scheduler=True)
