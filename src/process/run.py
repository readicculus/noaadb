import luigi

from process.tasks import PartitionAnnotationsTask

if __name__ == '__main__':
    luigi.build([PartitionAnnotationsTask()], local_scheduler=True)
