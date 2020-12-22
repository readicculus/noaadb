import os

import luigi

from core.tools import deps_tree
from pipelines.refine_ir import current_dir, pipelineConfig
from pipelines.refine_ir.tasks import GenerateKotzListTask, CreateRefinementDataTask, CreateStatsTask, \
    FilterRefinementsTask, CommitRefinementsTask

def staging():
    deps_tree(FilterRefinementsTask())
    luigi.build([FilterRefinementsTask()], local_scheduler=True)

if __name__ == '__main__':
    luigi_project_config = os.path.join(current_dir, 'refine_kotz.cfg')
    luigi.configuration.add_config_path(luigi_project_config)


    # luigi.build([CommitRefinementsTask()], local_scheduler=True)
