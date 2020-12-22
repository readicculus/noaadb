from pipelines.ingest.tasks.create_tables import CreateTableTask
from pipelines.ingest.tasks.ingest_kotz_images import IngestKotzImageDirectoryTask
from pipelines.ingest.tasks.ingest_chess_images import IngestCHESSImagesTask
from pipelines.ingest.tasks.ingest_chess_detections import IngestCHESSDetectionsTask
from pipelines.ingest.tasks.ingest_kotz_detections import IngestKotzDetectionsTask
from pipelines.ingest.tasks.ingest_pb_images import Ingest_pb_ru_ImagesTask, Ingest_pb_beufort_19_ImagesTask, Ingest_pb_us_ImagesTask
from pipelines.ingest.tasks.ingest_pb_detections import Ingest_pb_ru_DetectionsTask, Ingest_pb_beufort_19_DetectionsTask, Ingest_pb_us_DetectionsTask

from pipelines.ingest.tasks.ingest_all import IngestAllTask

__all__ = ["CreateTableTask", "IngestCHESSImagesTask", "IngestKotzImageDirectoryTask",
           "IngestCHESSDetectionsTask", "IngestKotzDetectionsTask",
           "Ingest_pb_ru_ImagesTask", "Ingest_pb_beufort_19_ImagesTask", "Ingest_pb_us_ImagesTask",
           "Ingest_pb_ru_DetectionsTask", "Ingest_pb_beufort_19_DetectionsTask", "Ingest_pb_us_DetectionsTask",
           "IngestAllTask"]
