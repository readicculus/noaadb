from ingest.tasks.create_tables import *
from ingest.tasks.ingest_chess_images import IngestCHESSImagesTask
from ingest.tasks.ingest_chess_detections import IngestCHESSDetectionsTask
from ingest.tasks.ingest_kotz_detections import IngestKotzDetectionsTask

__all__ = ["CreateTableTask", "IngestCHESSImagesTask",
           "IngestCHESSDetectionsTask", "IngestKotzDetectionsTask"]
