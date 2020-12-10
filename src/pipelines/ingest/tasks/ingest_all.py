import luigi

from pipelines.ingest.tasks import IngestKotzDetectionsTask, IngestCHESSDetectionsTask


class IngestAllTask(luigi.WrapperTask):
    def requires(self):
        yield IngestKotzDetectionsTask()
        yield IngestCHESSDetectionsTask()