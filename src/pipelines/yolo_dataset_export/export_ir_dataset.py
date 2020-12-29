import os

import luigi

from pipelines.yolo_dataset_export import pipeline_dir
from pipelines.yolo_dataset_export.tasks import ExportYoloIRDatasetTask

if __name__ == '__main__':
    luigi_project_config = os.path.join(pipeline_dir, 'export_ir_dataset.cfg')
    luigi.configuration.add_config_path(luigi_project_config)
    # luigi.build([ExportYoloIRDatasetTask()], local_scheduler=True)
    ExportYoloIRDatasetTask().generate_stats()