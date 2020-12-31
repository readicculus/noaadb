import os

import luigi

from pipelines.yolo_dataset_export import pipeline_dir
from pipelines.yolo_dataset_export.tasks import ExportYoloEODatasetTask, BatchImageAnnotationInformation

def load_pb_config():
    luigi_project_config = os.path.join(pipeline_dir, 'export_eo_pb_dataset.cfg')
    luigi.configuration.add_config_path(luigi_project_config)
def load_seal_config():
    luigi_project_config = os.path.join(pipeline_dir, 'export_eo_dataset.cfg')
    luigi.configuration.add_config_path(luigi_project_config)

if __name__ == '__main__':
    # luigi_project_config = os.path.join(pipeline_dir, 'export_eo_dataset.cfg')
    load_seal_config()
    # luigi.build([ExportYoloEODatasetTask()], workers=8)
    # ExportYoloEODatasetTask().generate_stats()
    luigi.build([ExportYoloEODatasetTask()], local_scheduler=True)
    # luigi.build([BatchImageAnnotationInformation()], local_scheduler=True)
