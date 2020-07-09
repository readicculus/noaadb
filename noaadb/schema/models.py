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
class ImageType(enum.IntEnum):
    EO = 1
    IR = 2
    FUSED = 3
    ALL = 4

class LabelType(enum.IntEnum):
    TP = 1
    FP = 2

class MLType(enum.IntEnum):
    TRAIN = 1
    TEST = 2

class FM_HEADER_FRAME_ID_TYPE(enum.IntEnum):
    ins_evt = 1
    ins = 2


JOBWORKERNAME = VARCHAR(50)
FILENAME = VARCHAR(100)
FILEPATH = VARCHAR(400)
sd_meta = MetaData(schema='survey_data')
SurveyDataBase = declarative_base(metadata=sd_meta)
label_meta = MetaData(schema='label_data')
LabelBase = declarative_base(metadata=label_meta)
ml_meta = MetaData(schema='ml')
MLBase = declarative_base(metadata=ml_meta)

####
# Survey schema models
####

class Survey(SurveyDataBase):
    __tablename__ = 'survey'
    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(String(50), unique=True)


class Flight(SurveyDataBase):
    __tablename__ = 'flight'
    id = Column(Integer, autoincrement=True, primary_key=True)
    flight_name = Column(String(50), unique=True)

    survey_id = Column(Integer, ForeignKey(Survey.id))
    survey = relationship(Survey)
    __table_args__ = (
        UniqueConstraint(survey_id, flight_name,
                         name='flight_unique_constraint'),
    )



class Camera(SurveyDataBase):
    __tablename__ = 'camera'
    id = Column(Integer, autoincrement=True, primary_key=True)
    cam_name = Column(String(20))
    flight_id = Column(Integer, ForeignKey(Flight.id))
    flight = relationship(Flight)
    __table_args__ = (
        UniqueConstraint(flight_id,cam_name,
                         name='cam_unique_constraint'),
    )


class HeaderMeta(SurveyDataBase):
    __tablename__ = 'header_meta'
    id = Column(Integer, autoincrement=True, primary_key=True)
    stamp = Column(BigInteger)
    frame_id = Column(VARCHAR(10))
    seq = Column(Integer)

    camera_id = Column(Integer, ForeignKey(Camera.id,
                             ondelete="CASCADE"))
    camera = relationship("Camera")
    __table_args = (
        UniqueConstraint(camera_id, stamp, name='_one_stamp_per_cam_header_meta'),
    )


class InstrumentMeta(SurveyDataBase):
    __tablename__ = 'instrument_meta'
    id = Column(Integer, autoincrement=True, primary_key=True)
    track_angle = Column(Float)
    angular_rate_x = Column(Float)
    angular_rate_y = Column(Float)
    angular_rate_z = Column(Float)
    down_velocity = Column(Float)
    pitch = Column(Float)
    altitude = Column(Float)
    north_velocity = Column(Float)
    acceleration_y = Column(Float)
    gnss_status = Column(Integer)
    longitude = Column(Float)
    roll = Column(Float)
    acceleration_x = Column(Float)
    align_status = Column(Integer)
    total_speed = Column(Float)
    time = Column(Float)
    latitude = Column(Float)
    heading = Column(Float)
    east_velocity = Column(Float)
    acceleration_z = Column(Float)
    header_meta_id = Column(Integer, ForeignKey(HeaderMeta.id,
                             ondelete="CASCADE"), unique=True, nullable=False)
    header_meta = relationship("HeaderMeta")#, backref=backref('ins', uselist=False, lazy='select'))



class FlightMetaEvent(SurveyDataBase):
    __tablename__ = 'evt_meta'
    id = Column(Integer, autoincrement=True, primary_key=True)
    event_port = Column(Integer)
    event_num = Column(Integer)
    time = Column(Float)

    header_meta_id = Column(Integer, ForeignKey(HeaderMeta.id,
                             ondelete="CASCADE"), nullable=False, unique=True)
    header_meta = relationship("HeaderMeta")#, backref=backref('evt', uselist=False, lazy='select'))

class Homography(SurveyDataBase):
    __tablename__ = 'homography'

    id = Column(Integer, autoincrement=True, primary_key=True)
    h00 = Column(Float, nullable=False)
    h01 = Column(Float, nullable=False)
    h02 = Column(Float, nullable=False)
    h10 = Column(Float, nullable=False)
    h11 = Column(Float, nullable=False)
    h12 = Column(Float, nullable=False)
    h20 = Column(Float, nullable=False)
    h21 = Column(Float, nullable=False)
    h22 = Column(Float, nullable=False)
    file_name = Column(FILENAME, nullable=False)
    file_path = Column(FILEPATH)

    camera_id = Column(Integer, ForeignKey(Camera.id, ondelete="CASCADE"), nullable=False)
    camera = relationship("Camera")

    @hybrid_property
    def matrix(self):
        return np.array([[self.h00,self.h01,self.h02],
                         [self.h10,self.h11,self.h12],
                         [self.h20,self.h21,self.h22]])


class EOImage(SurveyDataBase):
    __tablename__ = 'eo_image'
    file_name = Column(FILENAME, primary_key=True)
    file_path = Column(FILEPATH, nullable=False)
    type = Column(ENUM(ImageType, name="im_type_enum", metadata=sd_meta, create_type=True), nullable=False)
    foggy = Column(BOOLEAN)
    quality = Column(Integer)
    width = Column(Integer, nullable=False)
    height = Column(Integer, nullable=False)
    depth = Column(Integer, nullable=False)
    timestamp = Column(DateTime(timezone=True))

    is_bigendian = Column(BOOLEAN)
    step = Column(Integer)
    encoding = Column(VARCHAR(20))
    header_meta_id = Column(Integer, ForeignKey(HeaderMeta.id, ondelete="CASCADE"), unique=True, nullable=False)
    header_meta = relationship("HeaderMeta")#, backref=backref('eo_image', uselist=False, lazy='select'))




class IRImage(SurveyDataBase):
    __tablename__ = 'ir_image'
    file_name = Column(FILENAME, primary_key=True)
    file_path = Column(FILEPATH, nullable=False)
    type = Column(ENUM(ImageType, name="im_type_enum", metadata=sd_meta, create_type=False), nullable=False)
    foggy = Column(BOOLEAN)
    quality = Column(Integer)
    width = Column(Integer, nullable=False)
    height = Column(Integer, nullable=False)
    depth = Column(Integer, nullable=False)
    timestamp = Column(DateTime(timezone=True))

    is_bigendian = Column(BOOLEAN)
    step = Column(Integer)
    encoding = Column(VARCHAR(20))
    header_meta_id = Column(Integer, ForeignKey(HeaderMeta.id, ondelete="CASCADE"), unique=True, nullable=False)
    header_meta = relationship("HeaderMeta")#, backref=backref('ir_image', uselist=False, lazy='select'))

class HeaderGroup(SurveyDataBase):
    __tablename__ = 'header_group'
    id = Column(Integer, autoincrement=True, primary_key=True)
    eo_image_id = Column(FILENAME, ForeignKey(EOImage.file_name))
    eo_image = relationship(EOImage)
    ir_image_id = Column(FILENAME, ForeignKey(IRImage.file_name))
    ir_image = relationship(IRImage)
    evt_header_id = Column(Integer, ForeignKey(FlightMetaEvent.id, ondelete="CASCADE"))
    evt_header_meta = relationship("FlightMetaEvent")
####
# Label schema models
####
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

    # labels = relationship("LabelEntry", back_populates="sighting", cascade="all, delete-orphan")#,cascade="all, delete, delete-orphan" )

    def __repr__(self):
        return "<Hotspots(id='{}', hotspot_id='{}', species_id='{}', age_class='{}')>" \
            .format(self.id, self.hotspot_id, self.species_id, self.age_class)

class FalsePositiveSightings(Sighting):
    __mapper_args__ = {
        'polymorphic_identity':LabelType.FP,
    }
class TruePositiveSighting(Sighting):
    __mapper_args__ = {
        'polymorphic_identity':LabelType.TP,
    }



# @event.listens_for(Session, 'after_flush')
# def delete_tag_orphans(session, ctx):
#     session.query(Sighting).\
#         filter(~Sighting.labels.any()).\
#         delete(synchronize_session=False)





# V1.1 Models

# class ImageDimension(MLBase):
#     __tablename__ = 'image_dimensions'
#     id = Column(Integer,
#                 Sequence('im_dim_seq', start=1, increment=1, metadata=ml_meta),
#                 primary_key=True)
#     width = Column(Integer, nullable=False)
#     height = Column(Integer, nullable=False)
#     __table_args__ = (UniqueConstraint('width', 'height'),
#                       {'schema': "chips"})
#
#     def __repr__(self):
#         return "<ImageDimension(id='{}', width='{}', height='{}')>" \
#             .format(self.id, self.width, self.height)

# class Chip(MLBase):
#     __tablename__ = 'chip'
#     id = Column(Integer,
#                 Sequence('chip_seq', start=1, increment=1, metadata=ml_meta),
#                 primary_key=True)
#     image_dimension_id = Column(Integer, ForeignKey("ml.image_dimensions.id"), nullable=False)
#     image_dimension = relationship("ImageDimension")
#     width = Column(Integer, nullable=False)
#     height = Column(Integer, nullable=False)
#     overlap = Column(Integer, nullable=False)
#
#     x1 = Column(Integer, nullable=False)
#     y1 = Column(Integer, nullable=False)
#     x2 = Column(Integer, nullable=False)
#     y2 = Column(Integer, nullable=False)
#
#     @validates('x1','y1','x2','y2', 'image')
#     def chip_is_valid(self, key, f) -> str:
#         if key == 'y2' and self.y2 is not None and self.y1 > f:
#             raise ValueError('y1 > y2')
#         if key == 'x2' and self.x2 is not None and self.x1 > f:
#             raise ValueError('x1 > x2')
#         if key == 'x1' and self.x1 is not None and self.x1 < 0:
#             raise ValueError('x1 < 0')
#         if key == 'y1' and self.y1 is not None and self.y1 < 0:
#             raise ValueError('y1 < 0')
#         return f
#
#     def __repr__(self):
#         return "<Chip(id='{}', image_dimension='{}', width='{}', height='{}', overlap='{}', x1='{}', y1='{}', x2='{}', y2='{}')>" \
#             .format(self.id, self.image_dimension, self.width, self.height, self.overlap, self.x1, self.y1, self.x2, self.y2)
#
#
# class LabelChipBase(MLBase):
#     __abstract__=True
#     __table_args__ = {'schema': "ml"}
#
#     @declared_attr
#     def __tablename__(cls):
#         return cls.__name__.lower()
#
#     @declared_attr
#     def chip_id(cls):
#         return Column(Integer, ForeignKey("ml.chip.id"), nullable=False)
#
#     @declared_attr
#     def chip(cls):
#         return relationship("Chip")
#
#     @declared_attr
#     def label_id(cls):
#         return Column(Integer, ForeignKey("labels.eo_label.id"), nullable=False)
#
#     @declared_attr
#     def label(cls):
#         return relationship('EOLabelEntry', foreign_keys=[cls.label_id])
#
#     # @declared_attr
#     # def image_id(cls):
#     #     return Column(FILENAME, ForeignKey("noaa_surveys.images.file_name"), nullable=False)
#     #
#     # @declared_attr
#     # def image(cls):
#     #     return relationship('NOAAImage', foreign_keys=[cls.image_id])
#
#     percent_intersection = Column(Float, nullable=False)
#
#     @hybrid_property
#     def relative_x1(self): return self.label.x1 - self.chip.x1
#
#     @hybrid_property
#     def relative_x2(self): return self.chip.width - (self.chip.x2-self.label.x2)
#
#     @hybrid_property
#     def relative_y1(self): return self.label.y1 - self.chip.y1
#
#     @hybrid_property
#     def relative_y2(self): return self.chip.height - (self.chip.y2 - self.label.y2)
#
#
# class LabelChips(LabelChipBase):
#     __tablename__ = 'label_chips'
#     __table_args__ = {'schema': "chips"}
#     id = Column(Integer,
#                 Sequence('chip_hs_seq', start=1, increment=1, metadata=ml_meta),
#                 primary_key=True)
#
#     def __repr__(self):
#         return "<ChipHotspot(id='{}', relative_x1='{}', relative_x2='{}', relative_y1='{}', relative_y2='{}', chip_id='{}', label_id='{}', percent_intersection='{}')>" \
#             .format(self.id, self.relative_x1, self.relative_x2, self.relative_y1, self.relative_y2, self.chip_id, self.label_id, self.percent_intersection)
#
# class FPChips(LabelChipBase):
#     __tablename__ = 'fp_chips'
#     __table_args__ = {'schema': "chips"}
#     id = Column(Integer,
#                 Sequence('fp_chips_seq', start=1, increment=1, metadata=ml_meta),
#                 primary_key=True)
#
#
# class TrainTestSplit(MLBase):
#     __tablename__ = 'train_test_split'
#     id = Column(Integer,
#                 Sequence('train_test_seq', start=1, increment=1, metadata=ml_meta, schema="ml"),
#                 primary_key=True)
#     label_id = Column(Integer, ForeignKey("labels.labels.id"), nullable=False)
#     label = relationship('LabelEntry', foreign_keys=[label_id])
#     type = Column(ENUM(MLType, name="ml_type_enum", metadata=ml_meta, schema="ml", create_type=True), nullable=False)
#
#     def __repr__(self):
#         return "<TrainTestSplit(id='{}', label='{}', type='{}')>" \
#             .format(self.id, self.label, self.type)


# TABLE_DEPENDENCY_ORDER = [FlightMetaHeader,FlightMetaEvent, FlightMetaInstruments, NOAAImage,FlightCamId, Job,Worker,Species,Sighting, LabelEntry, IRLabelEntry, EOLabelEntry, ImageDimension, Chip, LabelChipBase, LabelChips, FPChips, TrainTestSplit]
