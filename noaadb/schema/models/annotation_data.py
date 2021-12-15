from sqlalchemy import Column, VARCHAR, BOOLEAN, ForeignKey, \
    Integer, Enum, UniqueConstraint
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.schema import CheckConstraint

from noaadb.schema import JOBWORKERNAME, FILENAME, FILEPATH
from noaadb.schema.models import NDB_Base
from noaadb.schema.models.survey_data import EOImage, IRImage

schema_name = 'annotation_data'
# label_meta = MetaData(schema=schema_name)
# DetectionBase = declarative_base(metadata=label_meta)
AnnotationBase = NDB_Base


class Job(AnnotationBase):
    __tablename__ = 'jobs'
    __table_args__ = {'schema': schema_name}
    name = Column(JOBWORKERNAME, nullable=False, unique=True, primary_key=True)
    file_path = Column(FILEPATH, nullable=False)
    notes = Column(VARCHAR(500))

    def __repr__(self):
        return "<Job(id='{}', job_name='{}', notes='{}')>" \
            .format(self.name, self.file_path, self.notes)


class Worker(AnnotationBase):
    __tablename__ = 'workers'
    __table_args__ = {'schema': schema_name}
    name = Column(JOBWORKERNAME, nullable=False, unique=True, primary_key=True)
    human = Column(BOOLEAN, nullable=False)

    def __repr__(self):
        return "<Worker(name='{}', human='{}')>" \
            .format(self.name, self.human)


class Species(AnnotationBase):
    __tablename__ = 'species'
    __table_args__ = {'schema': schema_name}

    id = Column(Integer, autoincrement=True,
                primary_key=True)
    name = Column(VARCHAR(100), nullable=False, unique=True)

    def __repr__(self):
        return "<Species(id='{}', name='{}')>" \
            .format(self.id, self.name)


class BoundingBox(AnnotationBase):
    __tablename__ = 'bounding_boxes'
    id = Column(Integer, autoincrement=True, primary_key=True)
    x1 = Column(Integer)
    x2 = Column(Integer)
    y1 = Column(Integer)
    y2 = Column(Integer)
    confidence = Column(VARCHAR(32))
    worker_id = Column(JOBWORKERNAME, ForeignKey(Worker.name), nullable=False)
    worker = relationship(Worker)
    job_id = Column(JOBWORKERNAME, ForeignKey(Job.name), nullable=False)
    job = relationship(Job)

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

    def pad(self, padding):
        self.x1 -= padding
        self.x2 += padding
        self.y1 -= padding
        self.y2 += padding

    def to_dict(self):
        return {'id': self.id,
                'x1': self.x1,
                'x2': self.x2,
                'y1': self.y1,
                'y2': self.y2,
                'area': self.area,
                'confidence': self.confidence}

    @classmethod
    def from_dict(cls, d):
        cls(x1=d['x1'],
            x2=d['x2'],
            y1=d['y1'],
            y2=d['y2'],
            confidence=d['confidence'],
            id=d.get('id'),
            worker_id=d.get('worker_id'),
            job_id=d.get('job_id'))
        return cls

    __table_args__ = (
        CheckConstraint('x1<=x2 AND y1<=y2',
                        name='bbox_valid'), {'schema': schema_name, },
    )


class Annotation(AnnotationBase):
    __tablename__ = 'annotation'
    __table_args__ = {'schema': schema_name}

    id = Column(Integer, autoincrement=True, primary_key=True)

    eo_event_key = Column(FILENAME, ForeignKey(EOImage.event_key, ondelete='CASCADE'))
    ir_event_key = Column(FILENAME, ForeignKey(IRImage.event_key, ondelete='CASCADE'))
    eo_image = relationship("EOImage", back_populates="annotations")
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
    ir_box = relationship(BoundingBox, foreign_keys=[ir_box_id], cascade="all,delete")
    eo_box_id = Column(Integer, ForeignKey(BoundingBox.id, ondelete='CASCADE'))
    eo_box = relationship(BoundingBox, foreign_keys=[eo_box_id], cascade="all,delete")

    @hybrid_property
    def species_name(self):
        return self.species.name

    @species_name.expression
    def species_name(cls):
        return cls.species.name

    def to_dict(self):
        ir_box_d = None if self.ir_box_id is None else self.ir_box.to_dict()
        eo_box_d = None if self.eo_box_id is None else self.eo_box.to_dict()
        d = {'species': self.species.name,
             'hotspot_id': self.hotspot_id,
             'age_class': self.age_class,
             'eo_event_key': self.eo_event_key,
             'ir_event_key': self.ir_event_key,
             'eo_box': eo_box_d,
             'ir_box': ir_box_d}
        return d



import enum


class TrainTestValidEnum(enum.Enum):
    train = 1
    test = 2
    valid = 3


class TrainTestValid(AnnotationBase):
    __tablename__ = 'train_test_valid'
    id = Column(Integer, autoincrement=True, primary_key=True)

    eo_event_key = Column(FILENAME, ForeignKey(EOImage.event_key, ondelete='CASCADE'), nullable=True)
    ir_event_key = Column(FILENAME, ForeignKey(IRImage.event_key, ondelete='CASCADE'), nullable=True)

    type = Column('type', Enum(TrainTestValidEnum))
    __table_args__ = (
        CheckConstraint('NOT(eo_event_key IS NULL AND ir_event_key IS NULL)'),
        UniqueConstraint(eo_event_key, ir_event_key, type),
        {'schema': schema_name},
    )


class Partitions(AnnotationBase):
    __tablename__ = 'image_partitions'
    id = Column(Integer, autoincrement=True, primary_key=True)

    eo_event_key = Column(FILENAME, ForeignKey(EOImage.event_key, ondelete='CASCADE'), nullable=True)
    ir_event_key = Column(FILENAME, ForeignKey(IRImage.event_key, ondelete='CASCADE'), nullable=True)

    partition = Column('partition', Integer)

    # @hybrid_property
    # def good(self):
    #     return object_session(self).query(exists().where(or_(IRWithoutErrors.ir_event_key==self.ir_event_key,
    #                                                          IRVerifiedBackground.ir_event_key==self.ir_event_key))).scalar()
    #
    # @good.expression
    # def good(cls):
    #     return (exists().where(or_(IRWithoutErrors.ir_event_key==cls.ir_event_key,
    #                                                          IRVerifiedBackground.ir_event_key==cls.ir_event_key)))

    # bad = column_property(select([exists().where(ir_event_key == IRWithErrors.ir_event_key)]))
    # good = column_property(exists().where(IRWithoutErrors.ir_event_key==ir_event_key))

    # is_background = Column('is_background', BOOLEAN) # TODO hybrid?
    __table_args__ = (
        CheckConstraint('NOT(eo_event_key IS NULL AND ir_event_key IS NULL)'),
        UniqueConstraint(eo_event_key, ir_event_key, partition),
        {'schema': schema_name},
    )

models = [Job, Worker, Species, BoundingBox, Annotation, Partitions]
__all__ = ["Job", "Worker", "Species", "BoundingBox", "Annotation", "Partitions", "models"]
