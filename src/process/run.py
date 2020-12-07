import luigi

from core.tools.deps_tree import print_tree
from process.tasks import MakeTestTrainValidTask

if __name__ == '__main__':
    print(print_tree(MakeTestTrainValidTask()))
    luigi.build([MakeTestTrainValidTask()], local_scheduler=True)
