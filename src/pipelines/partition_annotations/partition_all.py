import luigi

from core.tools.deps_tree import print_tree
from pipelines.partition_annotations.tasks import LoadTrainTestValidTask, PartitionAnnotationsTask

if __name__ == '__main__':
    print(print_tree(LoadTrainTestValidTask()))
    luigi.build([PartitionAnnotationsTask()], local_scheduler=True)
