import os

import luigi
from sqlalchemy import func

from core import ForcibleTask
from noaadb import Session
from noaadb.schema.models import Annotation, BoundingBox
from pipelines.refine_ir import pipelineConfig


class GenerateListTask(ForcibleTask):
    output_list = luigi.Parameter()
    max_size = luigi.IntParameter(default=-1)
    def output(self):
        conf = pipelineConfig()
        return luigi.LocalTarget(os.path.join(conf.output_root,self.output_list))

    def cleanup(self):
        if self.output().exists():
            self.output().remove()

class GenerateKotzListTask(GenerateListTask):
    def run(self):
        s = Session()
        counts = s.query(Annotation.ir_event_key, func.count(Annotation.ir_event_key))\
            .filter(Annotation.ir_event_key.ilike("%kotz%"))\
            .group_by(Annotation.ir_event_key).all()

        by_counts = sorted(counts, key=lambda x: x[1], reverse=True)
        s.close()
        with self.output().open('w') as f:
            if self.max_size != -1 and self.max_size < len(by_counts)-1:
                by_counts = by_counts[:self.max_size]
            for ir_key, count in by_counts:
                f.write("%s %d\n" % (ir_key, count))



class GeneratePoint2BoxListTask(GenerateListTask):
    def run(self):
        s = Session()
        counts = s.query(Annotation.ir_event_key, func.count(Annotation.ir_event_key))\
            .join(Annotation.ir_box)\
            .filter(BoundingBox.is_point)\
            .group_by(Annotation.ir_event_key).all()


        by_counts = sorted(counts, key=lambda x: x[1], reverse=True)
        s.close()
        with self.output().open('w') as f:
            for ir_key, count in by_counts:
                f.write("%s %d\n" % (ir_key, count))