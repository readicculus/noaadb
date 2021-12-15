import luigi

from core import AlwaysRunTask
from core.target import NOAADBTableTarget
from noaadb import DATABASE_URI
from noaadb.schema.models.survey_data import models as survey_models
from noaadb.schema.models.annotation_data import models as annotation_models

class PurgeTablesTask(AlwaysRunTask):
    purge_tables = luigi.BoolParameter(default=False)
    models = list(reversed(annotation_models)) + list(reversed(survey_models))
    targets = [NOAADBTableTarget(DATABASE_URI, model) for model in models]

    def complete(self):
        for target in self.targets:
            if target.exists():
                return False
        return True

    def run(self):
        if self.purge_tables:
            for t in self.targets:
                t.remove()

class CreateTablesTask(luigi.Task):
    purge_tables = luigi.BoolParameter(default=False)
    models = survey_models + annotation_models
    targets = [NOAADBTableTarget(DATABASE_URI, model) for model in models]

    def requires(self):
        return PurgeTablesTask(purge_tables=self.purge_tables)

    def run(self):
        for t in self.targets:
            t.touch()