import luigi
from core.tools.deps_tree import print_tree
from noaadb import Session
from noaadb.schema.models import Annotation, Species
from pipelines.ingest.tasks import Ingest_pb_ru_ImagesTask, Ingest_pb_ru_DetectionsTask, Ingest_pb_us_ImagesTask, \
    Ingest_pb_us_DetectionsTask, Ingest_pb_beufort_19_DetectionsTask

if __name__ == '__main__':
    print(print_tree(Ingest_pb_ru_DetectionsTask()))
    s = Session()
    eo_pb_count = s.query(Annotation).join(Species).filter(Species.name == "Polar Bear").filter(Annotation.eo_event_key != None).count()
    ir_pb_count = s.query(Annotation).join(Species).filter(Species.name == "Polar Bear").filter(Annotation.ir_event_key != None).count()
    s.close()
    a=1
    # luigi.build([Ingest_pb_beufort_19_DetectionsTask()], detailed_summary=True, local_scheduler=True)