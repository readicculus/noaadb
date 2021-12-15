from sqlalchemy import Column, Integer, VARCHAR, Float, ForeignKey, CheckConstraint
import sqlalchemy as sa
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from noaadb.schema import FILENAME
from noaadb.schema.models import NDB_Base
from noaadb.schema.models.annotation_data import Species
from noaadb.schema.models.survey_data import EOImage, IRImage

PseudoLabelBase = NDB_Base
schema_name = 'pseudo_labels'


class StandardBoundingBox(object):
    id = Column(Integer, autoincrement=True, primary_key=True)
    x1 = Column(Integer)
    x2 = Column(Integer)
    y1 = Column(Integer)
    y2 = Column(Integer)
    confidence = Column(Float)

    @hybrid_property
    def is_point(self): return self.x1 == self.x2 and self.y1 == self.y2

    @is_point.expression
    def is_point(cls): return cls.x1 == cls.x2 and cls.y1 == cls.y2

    @hybrid_property
    def cx(self): return int(self.x1 + (self.x2 - self.x1) / 2)

    @cx.expression
    def cx(cls): return int(cls.x1 + (cls.x2 - cls.x1) / 2)

    @hybrid_property
    def cy(self): return int(self.y1 + (self.y2 - self.y1) / 2)

    @cy.expression
    def cy(cls): return int(cls.y1 + (cls.y2 - cls.y1) / 2)

    @hybrid_property
    def width(self): return self.x2 - self.x1

    @width.expression
    def width(cls): return cls.x2 - cls.x1

    @hybrid_property
    def height(self): return self.y2 - self.y1

    @height.expression
    def height(cls): return cls.y2 - cls.y1

    @hybrid_property
    def area(self): return (self.y2 - self.y1) * (self.x2 - self.x1)

    @area.expression
    def area(cls): return (cls.y2 - cls.y1) * (cls.x2 - cls.x1)

    def iou(self, other):
        pass

    __table_args__ = (
        CheckConstraint('x1<=x2 AND y1<=y2 AND x1>=0 AND y1>=0',
                        name='bbox_valid_x1x2y1y2'),
        CheckConstraint('confidence >= 0 AND confidence <= 1',
                        name='bbox_valid_confidence'), {'schema': schema_name, },
    )


class StandardAnnotation(StandardBoundingBox):
    event_key = Column(FILENAME)

    species_id = Column(Integer, ForeignKey(Species.id), nullable=False)
    species = relationship(Species)

    ir_box_id = Column(Integer, ForeignKey(StandardBoundingBox.id, ondelete='CASCADE'))
    ir_box = relationship(StandardBoundingBox, foreign_keys=[ir_box_id], cascade="all,delete")
    eo_box_id = Column(Integer, ForeignKey(StandardBoundingBox.id, ondelete='CASCADE'))
    eo_box = relationship(StandardBoundingBox, foreign_keys=[eo_box_id], cascade="all,delete")

class LabelQueueItem(PseudoLabelBase):
    __tablename__ = 'lbl_queue'
    __table_args__ = {'schema': schema_name}
    id = Column(Integer, autoincrement=True, primary_key=True)
    x1 = Column(Integer)
    x2 = Column(Integer)
    y1 = Column(Integer)
    y2 = Column(Integer)
    model_confidence = Column(Float)
    worker_id = Column(JOBWORKERNAME, ForeignKey(Worker.name), nullable=False)
    worker = relationship(Worker)
    job_id = Column(JOBWORKERNAME, ForeignKey(Job.name), nullable=False)
    job = relationship(Job)