import logging

import luigi
from sqlalchemy import exists

from extras._partitions.tasks import UpdateTrainTestValidTask
from noaadb import Session
from noaadb.schema.models import *

class AppendBackgroundToPartitionsTask(luigi.Task):
    def output(self):
        return []

    def run(self):
        logger = logging.getLogger('luigi-interface')
        s = Session()
        images = s.query(IRImage.event_key, EOImage.event_key)\
            .join(IRVerifiedBackground, IRVerifiedBackground.ir_event_key == IRImage.event_key)\
            .join(EOImage, EOImage.event_key == IRImage.event_key)\
            .filter(~ exists().where(Annotation.ir_event_key==IRImage.event_key))\
            .filter(~ exists().where(Partitions.ir_event_key==IRImage.event_key))\
            .filter(~ exists().where(Partitions.eo_event_key==EOImage.event_key))\
            .all()

        n_partitions = s.query(Partitions.partition).distinct().all()
        new_partitions_dict = {k: [] for (k,) in n_partitions}
        partition_keys = list(new_partitions_dict.keys())
        partition_idx = 0
        while(len(images) > 0):
            k = partition_keys[partition_idx % len(partition_keys)]
            new_partitions_dict[k].append(images.pop())
            partition_idx += 1

        for partition_idx in new_partitions_dict:
            ps = new_partitions_dict[partition_idx]
            for ir_event_key, eo_event_key in ps:
                partition_obj = Partitions(ir_event_key=ir_event_key,
                                           eo_event_key=eo_event_key,
                                           partition=partition_idx)
                s.add(partition_obj)
        s.commit()
        s.close()
        for partition_idx in new_partitions_dict:
            logger.info("partition %d: added %d background images" % (partition_idx, len(new_partitions_dict[partition_idx])))
        x=1


@AppendBackgroundToPartitionsTask.event_handler(luigi.Event.SUCCESS)
def update_test_train_valid(task):
    luigi.build([UpdateTrainTestValidTask()], local_scheduler=True)


if __name__ == '__main__':
    # luigi_project_config = '../create_manual_review.cfg'
    # luigi.configuration.add_config_path(luigi_project_config)
    luigi.build([AppendBackgroundToPartitionsTask()], local_scheduler=True)

