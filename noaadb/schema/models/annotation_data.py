import enum
from typing import TypeVar

from sqlalchemy import Column, Date, VARCHAR, BOOLEAN, ForeignKey, \
    MetaData, Integer, Float
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.ext.declarative import declarative_base, declared_attr, ConcreteBase
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import validates, relationship
from sqlalchemy.schema import CheckConstraint, Sequence

from noaadb.schema import JOBWORKERNAME, FILENAME, FILEPATH
from noaadb.schema.models import Base

schema_name = 'annotation_data'
# label_meta = MetaData(schema=schema_name)
# DetectionBase = declarative_base(metadata=label_meta)
DetectionBase = Base

class Job(DetectionBase):
    __tablename__ = 'jobs'
    __table_args__ = {'schema': schema_name}
    name = Column(JOBWORKERNAME, nullable=False, unique=True, primary_key=True)
    file_path = Column(FILEPATH, nullable=False)
    notes = Column(VARCHAR(500))

    def __repr__(self):
        return "<Job(id='{}', job_name='{}', notes='{}')>" \
            .format(self.name, self.file_path, self.notes)


class Worker(DetectionBase):
    __tablename__ = 'workers'
    __table_args__ = {'schema': schema_name}
    name = Column(JOBWORKERNAME, nullable=False, unique=True, primary_key=True)
    human = Column(BOOLEAN, nullable=False)

    def __repr__(self):
        return "<Worker(name='{}', human='{}')>" \
            .format(self.name, self.human)


class Species(DetectionBase):
    __tablename__ = 'species'
    __table_args__ = {'schema': schema_name}

    id = Column(Integer, autoincrement=True,
                primary_key=True)
    name = Column(VARCHAR(100), nullable=False, unique=True)

    def __repr__(self):
        return "<Species(id='{}', name='{}')>" \
            .format(self.id, self.name)

class BoundingBox(DetectionBase):
    __tablename__ = 'bounding_boxes'
    id = Column(Integer, autoincrement=True, primary_key=True)
    x1 = Column(Integer)
    x2 = Column(Integer)
    y1 = Column(Integer)
    y2 = Column(Integer)
    confidence = Column(Float)
    worker_id = Column(JOBWORKERNAME, ForeignKey(Worker.name), nullable=False)
    worker = relationship(Worker)
    job_id = Column(JOBWORKERNAME, ForeignKey(Job.name), nullable=False)
    job = relationship(Job)

    __table_args__ = (
        CheckConstraint('x1<=x2 AND y1<=y2',
                        name='bbox_valid'), {'schema': schema_name,},
        )

class Annotation(DetectionBase):
    __tablename__ = 'annotation'
    __table_args__ = {'schema': schema_name}

    id = Column(Integer, autoincrement=True, primary_key=True)

    event_key = Column(FILENAME, nullable=False)
    # eo_image = relationship("EOImage", primaryjoin="foreign(Annotation.event_key)==EOImage.event_key")
    # ir_image = relationship("EOImage", primaryjoin="foreign(Annotation.event_key)==IRImage.event_key")
    # ir_image = relationship('IRImage', back_populates='labels',
    #              primaryjoin='Annotation.event_key==IRImage.event_key',
    #              foreign_keys='IRImage.event_key', remote_side='Annotation.event_key')
    # eo_image = relationship('EOImage', back_populates='labels',
    #                         primaryjoin='Annotation.event_key==EOImage.event_key',
    #                         foreign_keys='EOImage.event_key')
    species_id = Column(Integer, ForeignKey(Species.id), nullable=False)
    species = relationship(Species)

    hotspot_id = Column(VARCHAR(50))
    age_class = Column(VARCHAR(50))
    is_shadow = Column(BOOLEAN, nullable=True)

    ir_box_id = Column(Integer, ForeignKey(BoundingBox.id, ondelete='CASCADE'))
    ir_box = relationship(BoundingBox,foreign_keys=[ir_box_id])
    eo_box_id = Column(Integer, ForeignKey(BoundingBox.id, ondelete='CASCADE'))
    eo_box = relationship(BoundingBox,foreign_keys=[eo_box_id])


