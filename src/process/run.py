import luigi

from process.tasks import MakeTestTrainValidTask

if __name__ == '__main__':
    luigi.build([MakeTestTrainValidTask()], local_scheduler=True)
