import enum
from operator import and_

from sqlalchemy.ext.declarative import declarative_base, declared_attr, ConcreteBase, AbstractConcreteBase, \
    DeferredReflection
from sqlalchemy import Column, Date, VARCHAR, DateTime, BOOLEAN, ForeignKey, \
    MetaData, Integer, UniqueConstraint, Float, JSON, func, event, select, Unicode, String, BigInteger, \
    ForeignKeyConstraint, Index, ARRAY
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import validates, relationship, column_property, backref, aliased, with_polymorphic
from sqlalchemy.schema import CheckConstraint, Sequence
from sqlalchemy.dialects.postgresql import ENUM
import numpy as np
from sqlalchemy.orm.session import Session

from noaadb.schema import JOBWORKERNAME, FILEPATH, FILENAME
from noaadb.schema.models import IRImage, EOImage
from noaadb.schema.models.survey_data import ImageType

label_meta = MetaData(schema='label_data')
LabelBase = declarative_base(metadata=label_meta)



####
# Label schema models
####
class LabelType(enum.IntEnum):
    TP = 1
    FP = 2

class Job(LabelBase):
    __tablename__ = 'jobs'
    name = Column(JOBWORKERNAME, nullable=False, unique=True, primary_key=True)
    file_path = Column(FILEPATH, nullable=False)
    notes = Column(VARCHAR(500))

    def __repr__(self):
        return "<Job(id='{}', job_name='{}', notes='{}')>" \
            .format(self.name, self.file_path, self.notes)


class Worker(LabelBase):
    __tablename__ = 'workers'
    name = Column(JOBWORKERNAME, nullable=False, unique=True, primary_key=True)
    human = Column(BOOLEAN, nullable=False)

    def __repr__(self):
        return "<Worker(name='{}', human='{}')>" \
            .format(self.name, self.human)


class Species(LabelBase):
    __tablename__ = 'species'
    id = Column(Integer,
                Sequence('species_seq', start=1, increment=1, metadata=label_meta),
                primary_key=True)
    name = Column(VARCHAR(100), nullable=False, unique=True)

    def __repr__(self):
        return "<Species(id='{}', name='{}')>" \
            .format(self.id, self.name)

class LabelEntry(ConcreteBase, LabelBase):
    # __abstract__ = True
    __tablename__ = 'labels'
    # id = Column(Integer,
    #             Sequence('label_seq', start=1, increment=1, metadata=meta, schema="noaa_surveys"),
    #             primary_key=True)
    id = Column(Integer, autoincrement=True, primary_key=True)
    # @declared_attr
    # def id(cls):
    #     return Column(Integer, primary_key=True,autoincrement=True)

    # @declared_attr
    # def image_id(cls):
    #     return Column(FILENAME, ForeignKey(NOAAImage.file_name), nullable=False)
    #
    # @declared_attr
    # def image(cls):
    #     return relationship(NOAAImage)

    @declared_attr
    def job_id(cls):
        return Column(JOBWORKERNAME, ForeignKey(Job.name), nullable=False)
    @declared_attr
    def job(cls):
        return relationship(Job)

    @declared_attr
    def worker_id(cls):
        return Column(JOBWORKERNAME, ForeignKey(Worker.name), nullable=False)

    @declared_attr
    def worker(cls):
        return relationship(Worker)

    @declared_attr
    def species_id(cls):
        return Column(Integer, ForeignKey(Species.id), nullable=False)

    @declared_attr
    def species(cls):
        return relationship(Species)

    x1 = Column(Integer)
    x2 = Column(Integer)
    y1 = Column(Integer)
    y2 = Column(Integer)

    confidence = Column(Float)
    is_shadow = Column(BOOLEAN, nullable=True)
    start_date = Column(Date)
    end_date = Column(Date)

    __table_args__ = (
        CheckConstraint('x1<=x2 AND y1<=y2',
                        name='bbox_valid'),
        )


    discriminator = Column('label_type', ENUM(ImageType, name="im_type_enum", metadata=label_meta, create_type=False))
    __mapper_args__ = {'polymorphic_on': discriminator
                       }


    @validates('x1', 'x2', 'y1', 'y2')
    def validate_bbox(self, key, f) -> str:
        if key == 'y2' and self.y2 is not None and self.y1 > f:
            raise ValueError('y1 > y2')
        if key == 'x2' and self.x2 is not None and self.x1 > f:
            raise ValueError('x1 > x2')
        return f


class IRLabelEntry(LabelEntry):
    row_id = Column(Integer, primary_key=True,autoincrement=True)
    id = Column(Integer, ForeignKey(LabelEntry.id, ondelete="CASCADE"), unique=True)
    image_id = Column(FILENAME, ForeignKey(IRImage.file_name), nullable=False)
    image = relationship(IRImage)
    __tablename__ = 'ir_label'
    __mapper_args__ = {
    'polymorphic_identity':ImageType.IR,
        'polymorphic_load': 'inline',
        'inherit_condition': id == LabelEntry.id,
                       'with_polymorphic': '*'
    }


class EOLabelEntry(LabelEntry):
    row_id = Column(Integer, primary_key=True,autoincrement=True)
    id = Column(Integer, ForeignKey(LabelEntry.id, ondelete="CASCADE"),unique=True)
    image_id = Column(FILENAME, ForeignKey(EOImage.file_name), nullable=False)
    image = relationship(EOImage)
    __tablename__ = 'eo_label'
    __mapper_args__ = {
    'polymorphic_identity':ImageType.EO,
        'polymorphic_load': 'inline',
        'inherit_condition': id == LabelEntry.id,
        'with_polymorphic': '*'
    }
    # __table_args__ = (
    #     UniqueConstraint('confidence', 'x1', 'x2', 'y1', 'y2', 'image_id', 'species_id',
    #                      name='eo_label_unique_constraint'),
    # )
class Sighting(LabelBase):
    __tablename__ = 'sightings'
    id = Column(Integer,
                Sequence('sightings_seq', start=1, increment=1, metadata=label_meta),
                primary_key=True)

    hotspot_id = Column(VARCHAR(50))

    age_class = Column(VARCHAR(50))
    ir_label_id = Column(Integer, ForeignKey(IRLabelEntry.id,ondelete="CASCADE"))
    eo_label_id = Column(Integer, ForeignKey(EOLabelEntry.id,ondelete="CASCADE"))

    ir_label = relationship("IRLabelEntry", foreign_keys=[ir_label_id])#, backref=backref('sightings', cascade='all,delete'))
    eo_label = relationship("EOLabelEntry", foreign_keys=[eo_label_id])#, backref=backref('sightings', cascade='all,delete'))

    discriminator = Column('label_type', ENUM(LabelType, name="label_type_enum", metadata=label_meta, create_type=True))
    __mapper_args__ = {'polymorphic_on': discriminator}

class FalsePositiveSightings(Sighting):
    __mapper_args__ = {
        'polymorphic_identity':LabelType.FP,
    }
class TruePositiveSighting(Sighting):
    __mapper_args__ = {
        'polymorphic_identity':LabelType.TP,
    }
