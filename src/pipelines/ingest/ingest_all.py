import luigi

from pipelines.ingest.tasks import CreateTablesTask
from pipelines.ingest.tasks import IngestAllTask

if __name__ == '__main__':
    luigi.build([CreateTablesTask()], detailed_summary=True, local_scheduler=True)
    luigi.build([IngestAllTask()], detailed_summary=True, local_scheduler=True)