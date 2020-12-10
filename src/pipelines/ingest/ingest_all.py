import luigi
from core.tools.deps_tree import print_tree
from pipelines.ingest.tasks.ingest_all import IngestAllTask



if __name__ == '__main__':
    print(print_tree(IngestAllTask()))
    luigi.build([IngestAllTask()], detailed_summary=True, local_scheduler=True)