import luigi

from extras._partitions.tasks import UpdateTrainTestValidTask

if __name__ == '__main__':
    # luigi_project_config = '../create_manual_review.cfg'
    # luigi.configuration.add_config_path(luigi_project_config)
    luigi.build([UpdateTrainTestValidTask()], local_scheduler=True)

