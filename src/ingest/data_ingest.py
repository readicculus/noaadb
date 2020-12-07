import luigi
from core.tools.deps_tree import print_tree
from ingest.tasks import IngestKotzDetectionsTask, IngestCHESSDetectionsTask

if __name__ == '__main__':
    print(print_tree(IngestCHESSDetectionsTask()))
    print(print_tree(IngestKotzDetectionsTask()))
    luigi.build([IngestKotzDetectionsTask(), IngestCHESSDetectionsTask()], detailed_summary=True)