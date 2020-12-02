from ingest.tasks.create_tables import *
from ingest.tasks.ingest_chess_images import AggregateCHESSImagesTask
from ingest.tasks.ingest_kotz_images import AggregateKotzImagesTask
from ingest.tasks.ingest_chess_detections import LoadCHESSDetectionsTask
from ingest.tasks.ingest_kotz_detections import IngestKotzDetectionsTask

__all__ = ["CreateTableTask", "AggregateKotzImagesTask", "AggregateCHESSImagesTask",
           "LoadCHESSDetectionsTask", "IngestKotzDetectionsTask"]
