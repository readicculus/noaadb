import enum

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Date, VARCHAR, DateTime, BOOLEAN, ForeignKey, \
    MetaData, Integer, UniqueConstraint, Float, JSON, func, event
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import validates, relationship, column_property
from sqlalchemy.schema import CheckConstraint, Sequence
from sqlalchemy.dialects.postgresql import ENUM

class ImageType(enum.IntEnum):
    RGB = 1
    IR = 2
    UV = 3

FILEPATH = VARCHAR(400)
meta = MetaData()
Base = declarative_base(metadata=meta)

class NOAAImage(Base):
    __tablename__ = 'noaa_images'
    __table_args__ = {'schema': "noaa_labels"}
    abstract=True
    id = Column(Integer,
                Sequence('image_seq', start=1, increment=1, metadata=meta),
                primary_key=True)
    file_name = Column(VARCHAR(200), nullable=False, unique=True)
    file_path = Column(FILEPATH, nullable=False, unique=True)
    type = Column(ENUM(ImageType, name="im_type_enum", create_type=True), nullable=False)
    foggy = Column(BOOLEAN)
    quality = Column(Integer)
    width = Column(Integer, nullable=False)
    height = Column(Integer, nullable=False)
    depth = Column(Integer, nullable=False)
    flight = Column(VARCHAR(50))
    survey = Column(VARCHAR(100))
    timestamp = Column(DateTime(timezone=True))
    cam_position = Column(VARCHAR(100))
    meta = Column(JSON)
    # image_dimension_id = Column(Integer, ForeignKey("im_to_image_dimensions.id"), nullable=False)
    # image_dimension = relationship("ImageDimensions")

    def __repr__(self):
        return "<NOAAImage(id='{}', type='{}', foggy='{}', quality='{}', width='{}', height='{}', depth='{}', flight='{}', time='{}', file_name='{},'" \
               " survey='{}', timestamp='{}', cam_position='{}')>" \
            .format(self.id, self.type, self.foggy, self.quality, self.width, self.height, self.depth, self.flight, self.timestamp, self.file_name
                    , self.survey, self.timestamp, self.cam_position)



class Job(Base):
    __tablename__ = 'jobs'
    __table_args__ = {'schema': "noaa_labels"}
    id = Column(Integer,
                Sequence('job_seq', start=1, increment=1, metadata=meta),
                primary_key=True)
    job_name = Column(VARCHAR(100), nullable=False, unique=True)
    file_path = Column(FILEPATH, nullable=False)
    notes = Column(VARCHAR(500))

    def __repr__(self):
        return "<Job(id='{}', job_name='{}', notes='{}')>" \
            .format(self.id, self.job_name, self.file_path, self.notes)


class Worker(Base):
    __tablename__ = 'workers'
    __table_args__ = {'schema': "noaa_labels"}
    id = Column(Integer, Sequence('worker_seq', start=1, increment=1, metadata=meta),
                primary_key=True)
    name = Column(VARCHAR(100), nullable=False, unique=True)
    human = Column(BOOLEAN, nullable=False)

    def __repr__(self):
        return "<Worker(id='{}', name='{}', human='{}')>" \
            .format(self.id, self.name, self.human)


class Species(Base):
    __tablename__ = 'species'
    __table_args__ = {'schema': "noaa_labels"}
    id = Column(Integer,
                Sequence('species_seq', start=1, increment=1, metadata=meta),
                primary_key=True)
    name = Column(VARCHAR(100), nullable=False, unique=True)

    def __repr__(self):
        return "<Species(id='{}', name='{}')>" \
            .format(self.id, self.name)


class Label(Base):
    __tablename__ = 'labels'
    id = Column(Integer,
                Sequence('label_seq', start=1, increment=1, metadata=meta),
                primary_key=True)
    image_id = Column(Integer, ForeignKey('noaa_labels.noaa_images.id'), nullable=False)
    image = relationship("NOAAImage")
    species_id = Column(Integer, ForeignKey('noaa_labels.species.id'), nullable=False)
    species = relationship("Species")
    x1 = Column(Integer)
    x2 = Column(Integer)
    y1 = Column(Integer)
    y2 = Column(Integer)
    age_class = Column(VARCHAR(50))
    confidence = Column(Float)
    is_shadow = Column(BOOLEAN, nullable=False)
    start_date = Column(Date)
    end_date = Column(Date)
    hotspot_id = Column(VARCHAR(50))
    worker_id = Column(Integer, ForeignKey('noaa_labels.workers.id'), nullable=False)
    worker = relationship("Worker")
    job_id = Column(Integer, ForeignKey('noaa_labels.jobs.id'), nullable=False)
    job = relationship("Job") #, lazy="joined"

    __table_args__ = (
        CheckConstraint('x1<=x2 AND y1<=y2',
                        name='bbox_valid'),
        UniqueConstraint('x1', 'x2', 'y1', 'y2', 'image_id', 'species_id', name='_label_unique_constraint'),
        {'schema': "noaa_labels"}
        )

    @validates('x1','x2','y1','y2')
    def validate_bbox(self, key, f) -> str:
        if key == 'y2' and self.y2 is not None and self.y1 > f:
            raise ValueError('y1 > y2')
        if key == 'x2' and self.x2 is not None and self.x1 > f:
            raise ValueError('x1 > x2')
        return f
    def __repr__(self):
        return "<Label(id='{}', image_id='{}', species_id='{}', x1='{}', x2='{}'," \
               " y1='{}', y2='{}', age_class='{}', confidence='{}', is_shadow='{}', start_date='{}'," \
               " end_date='{}', hotspot_id_id='{}', worker_id='{}', manifest='{}')>" \
            .format(self.id, self.image_id, self.species_id, self.x1, self.x2, self.y1, self.y2,
                    self.age_class, self.confidence, self.is_shadow, self.start_date, self.end_date, self.hotspot_id, self.worker_id, self.job_id)


class Hotspot(Base):
    __tablename__ = 'hotspots'
    id = Column(Integer,
                Sequence('hotspot_seq', start=1, increment=1, metadata=meta),
                primary_key=True)
    eo_label_id = Column(Integer, ForeignKey('noaa_labels.labels.id'), unique=True, nullable=True)
    eo_label = relationship('Label', foreign_keys=[eo_label_id], lazy="joined")
    ir_label_id = Column(Integer, ForeignKey('noaa_labels.labels.id'), unique=True, nullable=True)
    ir_label = relationship('Label', foreign_keys=[ir_label_id], lazy="joined")
    hs_id = Column(VARCHAR(50))
    eo_accepted = Column(BOOLEAN, default=False)
    ir_accepted = Column(BOOLEAN, default=False)
    __table_args__ = (
        UniqueConstraint('eo_label_id', 'ir_label_id'),
        {'schema': "noaa_labels"}
        )
    def __repr__(self):
        return "<Hotspots(id='{}', eo_label_id='{}', ir_label_id='{}', hs_id='{}', eo_finalized='{}', ir_finalized='{}')>" \
            .format(self.id, self.eo_label_id, self.ir_label_id, self.hs_id, self.eo_accepted, self.ir_accepted)


class FalsePositives(Base):
    __tablename__ = 'falsepositives'
    id = Column(Integer,
                Sequence('fp_seq', start=1, increment=1, metadata=meta),
                primary_key=True)
    eo_label_id = Column(Integer, ForeignKey('noaa_labels.labels.id'), unique=True, nullable=True)
    eo_label = relationship('Label', foreign_keys=[eo_label_id])
    ir_label_id = Column(Integer, ForeignKey('noaa_labels.labels.id'), unique=True, nullable=True)
    ir_label = relationship('Label', foreign_keys=[ir_label_id])
    hs_id = Column(VARCHAR(50))
    __table_args__ = (
        UniqueConstraint('eo_label_id', 'ir_label_id'),
        {'schema': "noaa_labels"}
        )
    def __repr__(self):
        return "<FalsePositives(id='{}', eo_label_id='{}', ir_label_id='{}', hs_id='{}'')>" \
            .format(self.id, self.eo_label_id, self.ir_label_id, self.hs_id)


# V1.1 Models

class ImageDimension(Base):
    __tablename__ = 'image_dimensions'
    id = Column(Integer,
                Sequence('im_dim_seq', start=1, increment=1, metadata=meta, schema="chips"),
                primary_key=True)
    width = Column(Integer, nullable=False)
    height = Column(Integer, nullable=False)
    __table_args__ = (UniqueConstraint('width', 'height'),
                      {'schema': "chips"})

    def __repr__(self):
        return "<ImageDimension(id='{}', width='{}', height='{}')>" \
            .format(self.id, self.width, self.height)

class Chip(Base):
    __tablename__ = 'chip'
    __table_args__ = {'schema': "chips"}
    id = Column(Integer,
                Sequence('chip_seq', start=1, increment=1, metadata=meta, schema="chips"),
                primary_key=True)
    image_dimension_id = Column(Integer, ForeignKey("chips.image_dimensions.id"), nullable=False)
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

class LabelChips(Base):
    __tablename__ = 'label_chips'
    __table_args__ = {'schema': "chips"}
    id = Column(Integer,
                Sequence('chip_hs_seq', start=1, increment=1, metadata=meta, schema="chips"),
                primary_key=True)
    chip_id = Column(Integer, ForeignKey("chips.chip.id"), nullable=False)
    chip = relationship("Chip")
    label_id = Column(Integer, ForeignKey("noaa_labels.labels.id"), nullable=False)
    label = relationship('Label', foreign_keys=[label_id])
    percent_intersection = Column(Float, nullable=False)
    @hybrid_property
    def relative_x1(self): return self.label.x1 - self.chip.x1

    @hybrid_property
    def relative_x2(self): return self.chip.width - (self.chip.x2-self.label.x2)

    @hybrid_property
    def relative_y1(self): return self.label.y1 - self.chip.y1

    @hybrid_property
    def relative_y2(self): return self.chip.height - (self.chip.y2 - self.label.y2)

    def __repr__(self):
        return "<ChipHotspot(id='{}', relative_x1='{}', relative_x2='{}', relative_y1='{}', relative_y2='{}', chip_id='{}', label_id='{}', percent_intersection='{}')>" \
            .format(self.id, self.relative_x1, self.relative_x2, self.relative_y1, self.relative_y2, self.chip_id, self.label_id, self.percent_intersection)