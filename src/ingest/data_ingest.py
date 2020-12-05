import luigi
from luigi.tools import deps_tree

from ingest.tasks import IngestKotzDetectionsTask, IngestCHESSDetectionsTask

if __name__ == '__main__':
    print(deps_tree.print_tree(IngestCHESSDetectionsTask()))
    luigi.build([IngestKotzDetectionsTask(), IngestCHESSDetectionsTask()], local_scheduler=True)