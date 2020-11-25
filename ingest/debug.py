import luigi
from dotenv import load_dotenv, find_dotenv

from ingest.tasks.ingest_kotz_detections import IngestKotzDetectionsTask
from ingest.tasks.ingest_kotz_images import *
from ingest.tasks.ingest_chess_images import *

if __name__ == '__main__':
    load_dotenv(find_dotenv())

    luigi.build([IngestKotzDetectionsTask()], local_scheduler=True)
    # luigi.build([AggregateCHESSImagesTask(), AggregateKotzImagesTask()], local_scheduler=True)
