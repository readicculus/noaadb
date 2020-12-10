from pipelines.ingest.tasks.create_tables import CreateTableTask
from pipelines.ingest.tasks.ingest_kotz_images import IngestKotzImageDirectoryTask
from pipelines.ingest.tasks.ingest_chess_images import IngestCHESSImagesTask
from pipelines.ingest.tasks.ingest_chess_detections import IngestCHESSDetectionsTask
from pipelines.ingest.tasks.ingest_kotz_detections import IngestKotzDetectionsTask
from pipelines.ingest.tasks.ingest_all import IngestAllTask

__all__ = ["CreateTableTask", "IngestCHESSImagesTask", "IngestKotzImageDirectoryTask",
           "IngestCHESSDetectionsTask", "IngestKotzDetectionsTask", "IngestAllTask"]
