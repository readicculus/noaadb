from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Date, VARCHAR, DateTime, BOOLEAN, ForeignKey, \
    MetaData, Integer, UniqueConstraint
from sqlalchemy.orm import validates, relationship
from sqlalchemy.schema import CheckConstraint, Sequence
from sqlalchemy.dialects.postgresql import ENUM

from noaadb.schema.config import config

FILEPATH = VARCHAR(400)
meta = MetaData(schema=config["schema_name"])
Base = declarative_base(metadata=meta)

class NOAAImage(Base):
    __tablename__ = 'noaa_images'
    id = Column(Integer,
                Sequence('image_seq', start=1, increment=1, metadata=meta),
                primary_key=True)
    file_name = Column(VARCHAR(200), nullable=False, unique=True)
    file_path = Column(FILEPATH, nullable=False, unique=True)
    type = Column(ENUM("RGB", "IR", "UV", name="im_type_enum", create_type=True), nullable=False)
    foggy = Column(BOOLEAN)
    quality = Column(Integer)
    width = Column(Integer, nullable=False)
    height = Column(Integer, nullable=False)
    depth = Column(Integer, nullable=False)
    flight = Column(VARCHAR(50))
    survey = Column(VARCHAR(100))
    timestamp = Column(DateTime(timezone=True))
    cam_position = Column(VARCHAR(100))

    def __repr__(self):
        return "<NOAAImage(id='{}', type='{}', foggy='{}', quality='{}', width='{}', height='{}', depth='{}', flight='{}', time='{}', file_name='{},'" \
               " survey='{}', timestamp='{}', cam_position='{}')>" \
            .format(self.id, self.type, self.foggy, self.quality, self.width, self.height, self.depth, self.flight, self.timestamp, self.file_name
                    , self.survey, self.timestamp, self.cam_position)


class Chip(Base):
    __tablename__ = 'chips'
    id = Column(Integer,
                Sequence('chip_seq', start=1, increment=1, metadata=meta),
                primary_key=True)
    image_id = Column(Integer, ForeignKey("noaa_images.id"), nullable=False)
    image = relationship("NOAAImage")
    file_path = Column(FILEPATH)
    relative_x1 = Column(Integer, nullable=False)
    relative_y1 = Column(Integer, nullable=False)
    relative_x2 = Column(Integer, nullable=False)
    relative_y2 = Column(Integer, nullable=False)

    def __repr__(self):
        return "<Chip(id='{}', path='{}', relative_x1='{}', relative_y1='{}', relative_x2='{}', relative_y2='{}', image_id='{}')>" \
            .format(self.id, self.file_path, self.relative_x1, self.relative_y1, self.relative_x2, self.relative_y2, self.image_id)


class Job(Base):
    __tablename__ = 'jobs'
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
    id = Column(Integer, Sequence('worker_seq', start=1, increment=1, metadata=meta),
                primary_key=True)
    name = Column(VARCHAR(100), nullable=False, unique=True)
    human = Column(BOOLEAN, nullable=False)

    def __repr__(self):
        return "<Worker(id='{}', name='{}', human='{}')>" \
            .format(self.id, self.name, self.human)


class Species(Base):
    __tablename__ = 'species'
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
    image_id = Column(Integer, ForeignKey('noaa_images.id'), nullable=False)
    image = relationship("NOAAImage")
    species_id = Column(Integer, ForeignKey('species.id'), nullable=False)
    species = relationship("Species")
    x1 = Column(Integer)
    x2 = Column(Integer)
    y1 = Column(Integer)
    y2 = Column(Integer)
    age_class = Column(VARCHAR(50))
    confidence = Column(Integer)
    is_shadow = Column(BOOLEAN, nullable=False)
    start_date = Column(Date)
    end_date = Column(Date)
    hotspot_id = Column(VARCHAR(50))
    worker_id = Column(Integer, ForeignKey('workers.id'), nullable=False)
    worker = relationship("Worker")
    job_id = Column(Integer, ForeignKey('jobs.id'), nullable=False)
    job = relationship("Job")

    __table_args__ = (
        CheckConstraint('x1<=x2 AND y1<=y2',
                        name='bbox_valid'),
        UniqueConstraint('x1', 'x2', 'y1', 'y2', 'image_id', 'species_id', name='_label_unique_constraint'),
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

# class LabelHistory(Label):
#     __tablename__ = 'label_history'
#     __mapper_args__ = {'concrete':True}
#     id = Column(Integer, primary_key=True)
#     label_id = Column(Integer, ForeignKey('labels.id'), nullable=False)
#     def __repr__(self):
#         return "<LabelHistory(id='{}', image='{}', species='{}', x1='{}', x2='{}', y1='{}', y2='{}', age_class='{}', confidence='{}', is_shadow='{}', start_date='{}'," \
#                " end_date='{}', hotspot_id='{}', worker='{}', manifest='{}', label_id='{}')>" \
#             .format(self.id, self.image, self.species, self.x1, self.x2, self.y1, self.y2,
#                     self.age_class, self.confidence, self.is_shadow, self.start_date, self.end_date, self.hotspot_id, self.worker, self.job, self.label_id)

class Hotspot(Base):
    __tablename__ = 'hotspots'
    id = Column(Integer,
                Sequence('hotspot_seq', start=1, increment=1, metadata=meta),
                primary_key=True)
    eo_label_id = Column(Integer, ForeignKey('labels.id'), unique=True, nullable=True)
    eo_label = relationship('Label', foreign_keys=[eo_label_id])
    ir_label_id = Column(Integer, ForeignKey('labels.id'), unique=True, nullable=True)
    ir_label = relationship('Label', foreign_keys=[ir_label_id])
    hs_id = Column(VARCHAR(50))
    eo_accepted = Column(BOOLEAN, default=False)
    ir_accepted = Column(BOOLEAN, default=False)
    __table_args__ = (
        UniqueConstraint('eo_label_id', 'ir_label_id'),
        )
    def __repr__(self):
        return "<Hotspots(id='{}', eo_label_id='{}', ir_label_id='{}', hs_id='{}', eo_finalized='{}', ir_finalized='{}')>" \
            .format(self.id, self.eo_label_id, self.ir_label_id, self.hs_id, self.eo_accepted, self.ir_accepted)



