import luigi

from extras._partitions.tasks import UpdateTrainTestValidTask, UpdatePartitionsTask

if __name__ == '__main__':
    # luigi_project_config = '../create_manual_review.cfg'
    # luigi.configuration.add_config_path(luigi_project_config)
    luigi.build([UpdateTrainTestValidTask()], local_scheduler=True)
    # luigi.build([UpdatePartitionsTask()], local_scheduler=True)

