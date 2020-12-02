import luigi

from src.ingest.tasks.ingest_chess_detections import LoadCHESSDetectionsTask
from src.ingest.tasks.ingest_kotz_detections import IngestKotzDetectionsTask



if __name__ == '__main__':
    luigi.build([IngestKotzDetectionsTask(), LoadCHESSDetectionsTask()], local_scheduler=True)
