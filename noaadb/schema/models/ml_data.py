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


# V1.1 Models
class MLType(enum.IntEnum):
    TRAIN = 1
    TEST = 2

ml_schema_name = 'ml_data'
ml_meta = MetaData(schema='ml_data')
MLBase = declarative_base(metadata=ml_meta)
class ImageDimension(MLBase):
    __tablename__ = 'image_dimensions'
    id = Column(Integer,
                Sequence('im_dim_seq', start=1, increment=1, metadata=ml_meta),
                primary_key=True)
    width = Column(Integer, nullable=False)
    height = Column(Integer, nullable=False)
    __table_args__ = (UniqueConstraint('width', 'height'),
                      {'schema': "chips"})

    def __repr__(self):
        return "<ImageDimension(id='{}', width='{}', height='{}')>" \
            .format(self.id, self.width, self.height)

class Chip(MLBase):
    __tablename__ = 'chip'
    id = Column(Integer,
                Sequence('chip_seq', start=1, increment=1, metadata=ml_meta),
                primary_key=True)
    image_dimension_id = Column(Integer, ForeignKey("%s.image_dimensions.id" % ml_schema_name), nullable=False)
    image_dimension = relationship("ImageDimension")
    width = Column(Integer, nullable=False)
    height = Column(Integer, nullable=False)
    overlap = Column(Integer, nullable=False)

    x1 = Column(Integer, nullable=False)
    y1 = Column(Integer, nullable=False)
    x2 = Column(Integer, nullable=False)
    y2 = Column(Integer, nullable=False)

    @validates('x1','y1','x2','y2', 'image')
    def chip_is_valid(self, key, f) -> str:
        if key == 'y2' and self.y2 is not None and self.y1 > f:
            raise ValueError('y1 > y2')
        if key == 'x2' and self.x2 is not None and self.x1 > f:
            raise ValueError('x1 > x2')
        if key == 'x1' and self.x1 is not None and self.x1 < 0:
            raise ValueError('x1 < 0')
        if key == 'y1' and self.y1 is not None and self.y1 < 0:
            raise ValueError('y1 < 0')
        return f

    def __repr__(self):
        return "<Chip(id='{}', image_dimension='{}', width='{}', height='{}', overlap='{}', x1='{}', y1='{}', x2='{}', y2='{}')>" \
            .format(self.id, self.image_dimension, self.width, self.height, self.overlap, self.x1, self.y1, self.x2, self.y2)


class LabelChipBase(MLBase):
    __abstract__=True
    __table_args__ = {'schema': ml_schema_name}

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    @declared_attr
    def chip_id(cls):
        return Column(Integer, ForeignKey("%s.chip.id" % ml_schema_name), nullable=False)

    @declared_attr
    def chip(cls):
        return relationship("Chip")

    @declared_attr
    def label_id(cls):
        return Column(Integer, ForeignKey("labels.eo_label.id"), nullable=False)

    @declared_attr
    def label(cls):
        return relationship('EOLabelEntry', foreign_keys=[cls.label_id])

    # @declared_attr
    # def image_id(cls):
    #     return Column(FILENAME, ForeignKey("noaa_surveys.images.file_name"), nullable=False)
    #
    # @declared_attr
    # def image(cls):
    #     return relationship('NOAAImage', foreign_keys=[cls.image_id])

    percent_intersection = Column(Float, nullable=False)

    @hybrid_property
    def relative_x1(self): return self.label.x1 - self.chip.x1

    @hybrid_property
    def relative_x2(self): return self.chip.width - (self.chip.x2-self.label.x2)

    @hybrid_property
    def relative_y1(self): return self.label.y1 - self.chip.y1

    @hybrid_property
    def relative_y2(self): return self.chip.height - (self.chip.y2 - self.label.y2)


class LabelChips(LabelChipBase):
    __tablename__ = 'label_chips'
    __table_args__ = {'schema': ml_schema_name}
    id = Column(Integer,
                Sequence('chip_hs_seq', start=1, increment=1, metadata=ml_meta),
                primary_key=True)

class FPChips(LabelChipBase):
    __tablename__ = 'fp_chips'
    __table_args__ = {'schema': ml_schema_name}
    id = Column(Integer,
                Sequence('fp_chips_seq', start=1, increment=1, metadata=ml_meta),
                primary_key=True)


class TrainTestSplit(MLBase):
    __tablename__ = 'train_test_split'
    id = Column(Integer,
                Sequence('train_test_seq', start=1, increment=1, metadata=ml_meta, schema="ml"),
                primary_key=True)
    label_id = Column(Integer, ForeignKey("labels.labels.id"), nullable=False)
    label = relationship('LabelEntry', foreign_keys=[label_id])
    type = Column(ENUM(MLType, name="ml_type_enum", metadata=ml_meta, schema="ml", create_type=True), nullable=False)

    def __repr__(self):
        return "<TrainTestSplit(id='{}', label='{}', type='{}')>" \
            .format(self.id, self.label, self.type)
