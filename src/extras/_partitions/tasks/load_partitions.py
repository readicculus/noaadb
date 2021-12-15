import json

import luigi
from sqlalchemy import tuple_

from noaadb import Session
from noaadb.schema.models import Annotation, Partitions
from pipelines.partition_annotations.tasks import PartitionAnnotationsTask


class LoadPartitionsTask(luigi.Task):
    def requires(self):
        return {
            'partitions': PartitionAnnotationsTask(),
            '_table': CreateTableTask(["Partitions"])
        }

    def output(self):
        pass

    def run(self):
        keys = list(self.input()['partitions'].keys())
        num_partitions = 0
        partitions = {}

        for k in keys:
            if isinstance(k, int):
                num_partitions += 1
                with self.input()['partitions'][k].open('r') as f:
                    partitions[k] = json.loads(f.read())

        s = Session()
        for p_k in partitions:
            eo_ir_pairs = s.query(Annotation.eo_event_key, Annotation.ir_event_key) \
                .filter(Annotation.eo_event_key.in_(partitions[p_k])) \
                .distinct(tuple_(Annotation.eo_event_key, Annotation.ir_event_key)).all()

            for eo_k, ir_k in eo_ir_pairs:
                partition_obj = Partitions(eo_event_key=eo_k, ir_event_key=ir_k, partition=p_k)
                s.add(partition_obj)
        s.commit()
        s.close()


if __name__ == '__main__':
    # luigi_project_config = '../create_manual_review.cfg'
    # luigi.configuration.add_config_path(luigi_project_config)
    luigi.build([LoadPartitionsTask()], local_scheduler=True)

