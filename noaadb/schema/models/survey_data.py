import enum
from typing import TypeVar

import numpy as np
from sqlalchemy import Column, VARCHAR, DateTime, BOOLEAN, ForeignKey, \
    MetaData, Integer, UniqueConstraint, Float, String, BigInteger
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship


from noaadb.schema import FILENAME, FILEPATH

schema_name = 'survey_data'
# sd_meta = MetaData(schema=schema_name)
# SurveyDataBase = declarative_base(metadata=sd_meta)
Base = declarative_base()

SurveyDataBase = Base

####
# Survey schema models
####
class ImageType(enum.IntEnum):
    EO = 1
    IR = 2
    FUSED = 3
    ALL = 4

class Survey(SurveyDataBase):
    __tablename__ = 'survey'
    __table_args__ = {'schema': schema_name}
    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(String(50), unique=True)


class Flight(SurveyDataBase):
    __tablename__ = 'flight'
    id = Column(Integer, autoincrement=True, primary_key=True)
    flight_name = Column(String(50))

    survey_id = Column(Integer, ForeignKey(Survey.id))
    survey = relationship(Survey)
    __table_args__ = (
        UniqueConstraint(survey_id, flight_name,
                         name='flight_unique_constraint'),{'schema': schema_name},
    )


class Camera(SurveyDataBase):
    __tablename__ = 'camera'
    id = Column(Integer, autoincrement=True, primary_key=True)
    cam_name = Column(String(20))

    flight_id = Column(Integer, ForeignKey(Flight.id))
    flight = relationship(Flight)
    __table_args__ = (
        UniqueConstraint(flight_id,cam_name,
                         name='cam_unique_constraint'), {'schema': schema_name},
    )


class HeaderMeta(SurveyDataBase):
    __tablename__ = 'header_meta'
    __table_args__ = {'schema': schema_name}
    id = Column(Integer, autoincrement=True, primary_key=True)
    event_key = Column(FILENAME, primary_key=True)
    stamp = Column(BigInteger)
    frame_id = Column(VARCHAR(10))
    seq = Column(Integer)

    camera_id = Column(Integer, ForeignKey(Camera.id,
                             ondelete="CASCADE"))
    camera = relationship("Camera")
    # __table_args = (
    #     UniqueConstraint(camera_id, event_key,  name='_one_header_per_event'),
    # )


class InstrumentMeta(SurveyDataBase):
    __tablename__ = 'instrument_meta'
    __table_args__ = {'schema': schema_name}
    event_key = Column(FILENAME, primary_key=True)
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

# class EventMeta(SurveyDataBase):
#     __tablename__ = 'evt_meta'
#     id = Column(Integer, autoincrement=True, primary_key=True)
#     event_port = Column(Integer)
#     event_num = Column(Integer)
#     time = Column(Float)
#
#     header_meta_id = Column(Integer, ForeignKey(HeaderMeta.id,
#                              ondelete="CASCADE"), nullable=False, unique=True)
#     header_meta = relationship("HeaderMeta")#, backref=backref('evt', uselist=False, lazy='select'))

class Homography(SurveyDataBase):
    __tablename__ = 'homography'
    __table_args__ = {'schema': schema_name}

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
    __table_args__ = {'schema': schema_name}
    event_key = Column(FILENAME, primary_key=True)
    filename = Column(FILENAME)
    directory = Column(FILEPATH)
    # type = Column(ENUM(ImageType, name="im_type_enum", metadata=sd_meta, create_type=True), nullable=False)
    foggy = Column(BOOLEAN)
    quality = Column(Integer)
    width = Column(Integer, nullable=False)
    height = Column(Integer, nullable=False)
    depth = Column(Integer, nullable=False)
    timestamp = Column(DateTime(timezone=True))

    is_bigendian = Column(BOOLEAN)
    step = Column(Integer)
    encoding = Column(VARCHAR(20))
    camera_id = Column(Integer, ForeignKey(Camera.id,
                             ondelete="CASCADE"))
    camera = relationship("Camera")
    @hybrid_property
    def camera_name(self): return self.camera.cam_name
    @hybrid_property
    def flight(self): return self.camera.flight.flight_name
    # @hybrid_property
    # def survey(self):
    #     return self.camera.flight.survey.name

    # labels = relationship('Annotation', backref='eo_image',
    #              primaryjoin='Annotation.eo_event_key==EOImage.event_key',
    #              foreign_keys='Annotation.eo_event_key')

    def to_dict(self):
        res = {'w': self.width,
               'h': self.height,
               'c': self.depth,
               'filename': self.filename,
               'directory': self.directory,
               'event_key': self.event_key
               }
        return res

class IRImage(SurveyDataBase):
    __tablename__ = 'ir_image'
    __table_args__ = {'schema': schema_name}
    event_key = Column(FILENAME, primary_key=True)
    filename = Column(FILENAME)
    directory = Column(FILEPATH)
    # file_path = Column(FILEPATH, nullable=False)
    # type = Column(ENUM(ImageType, name="im_type_enum", metadata=sd_meta, create_type=False), nullable=False)
    foggy = Column(BOOLEAN)
    quality = Column(Integer)
    width = Column(Integer, nullable=False)
    height = Column(Integer, nullable=False)
    depth = Column(Integer, nullable=False)
    timestamp = Column(DateTime(timezone=True))

    is_bigendian = Column(BOOLEAN)
    step = Column(Integer)
    encoding = Column(VARCHAR(20))
    # labels = relationship('Annotation', backref='ir_image',
    #              primaryjoin='Annotation.ir_event_key==IRImage.event_key',
    #              foreign_keys='Annotation.ir_event_key')

    camera_id = Column(Integer, ForeignKey(Camera.id, ondelete="CASCADE"))
    camera = relationship("Camera")
    @hybrid_property
    def camera_name(self): return self.camera.cam_name
    @hybrid_property
    def flight(self): return self.camera.flight.flight_name
    # @hybrid_property
    # def survey(self):
    #     return self.camera.flight.survey.name
    def to_dict(self):
        res = {'w': self.width,
               'h': self.height,
               'c': self.depth,
               'filename': self.filename,
               'directory': self.directory,
               'event_key': self.event_key
               }
        return res



# class FusedImage(SurveyDataBase):
#     __tablename__ = 'fused_image'
#     file_name = Column(FILENAME, primary_key=True)
#     s3_uri = Column(FILEPATH, nullable=False)
#     file_path = Column(FILEPATH, nullable=False)
#
#     width = Column(Integer, nullable=False)
#     height = Column(Integer, nullable=False)
#     depth = Column(Integer, nullable=False)
#
#     eo_image_id = Column(FILENAME, ForeignKey(EOImage.file_name))
#     eo_image = relationship(EOImage)
#     ir_image_id = Column(FILENAME, ForeignKey(IRImage.file_name))
#     ir_image = relationship(IRImage)
#     homography_id = Column(Integer, ForeignKey(Homography.id))
#     homography = relationship(Homography)
#
#     def to_dict(self):
#         res = {'w': self.width,
#                'h': self.height,
#                'c': self.depth,
#                'file_name': self.file_name,
#                'file_path': self.file_path}
#         return res
#
# class HeaderGroup(SurveyDataBase):
#     __tablename__ = 'header_group'
#     id = Column(Integer, autoincrement=True, primary_key=True)
#     eo_image_id = Column(FILENAME, ForeignKey(EOImage.file_name, ondelete="CASCADE"))
#     eo_image = relationship(EOImage)
#     ir_image_id = Column(FILENAME, ForeignKey(IRImage.file_name, ondelete="CASCADE"))
#     ir_image = relationship(IRImage)
#     evt_header_id = Column(Integer, ForeignKey(EventMeta.id, ondelete="CASCADE"))
#     evt_header_meta = relationship("EventMeta")
#     ins_header_id = Column(Integer, ForeignKey(InstrumentMeta.id, ondelete="CASCADE"))
#     ins_header_meta = relationship("InstrumentMeta")


DBImage = TypeVar('DBImage', EOImage, IRImage)
