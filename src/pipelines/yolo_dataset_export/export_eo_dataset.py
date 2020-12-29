import os

import luigi

from pipelines.yolo_dataset_export import pipeline_dir
from pipelines.yolo_dataset_export.tasks import ExportYoloEODatasetTask, BatchImageAnnotationInformation

if __name__ == '__main__':
    luigi_project_config = os.path.join(pipeline_dir, 'export_eo_dataset.cfg')
    luigi.configuration.add_config_path(luigi_project_config)
    # luigi.build([ExportYoloEODatasetTask()], workers=2)
    luigi.build([ExportYoloEODatasetTask()], local_scheduler=True)
    # luigi.build([BatchImageAnnotationInformation()], local_scheduler=True)
