import enum
import os
from typing import TypeVar

import cv2
import numpy as np
from sqlalchemy import Column, VARCHAR, DateTime, BOOLEAN, ForeignKey, \
    Integer, UniqueConstraint, Float, String, BigInteger
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from noaadb.schema import FILENAME, FILEPATH
from noaadb.schema.models import NDB_Base

schema_name = 'survey_data'
# sd_meta = MetaData(schema=schema_name)
# SurveyDataBase = declarative_base(metadata=sd_meta)

SurveyBase = NDB_Base

####
# Survey schema models
####
class ImageType(enum.IntEnum):
    EO = 1
    IR = 2
    FUSED = 3
    ALL = 4

class Survey(SurveyBase):
    __tablename__ = 'survey'
    __table_args__ = {'schema': schema_name}
    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(String(50), unique=True)

class Flight(SurveyBase):
    __tablename__ = 'flight'
    id = Column(Integer, autoincrement=True, primary_key=True)
    flight_name = Column(String(50))

    survey_id = Column(Integer, ForeignKey(Survey.id))
    survey = relationship(Survey)
    __table_args__ = (
        UniqueConstraint(survey_id, flight_name,
                         name='flight_unique_constraint'),{'schema': schema_name},
    )


class Camera(SurveyBase):
    __tablename__ = 'camera'
    id = Column(Integer, autoincrement=True, primary_key=True)
    cam_name = Column(String(20))

    flight_id = Column(Integer, ForeignKey(Flight.id))
    flight = relationship(Flight)
    __table_args__ = (
        UniqueConstraint(flight_id,cam_name,
                         name='cam_unique_constraint'), {'schema': schema_name},
    )


class HeaderMeta(SurveyBase):
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


class InstrumentMeta(SurveyBase):
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

class Homography(SurveyBase):
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



class EOImage(SurveyBase):
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
    annotations = relationship("Annotation", back_populates="eo_image")

    def to_dict(self):
        res = {'w': self.width,
               'h': self.height,
               'c': self.depth,
               'filename': self.filename,
               'directory': self.directory,
               'event_key': self.event_key
               }
        return res

    def path(self):
        return os.path.join(self.directory, self.filename)

    def ocv_load(self):
        return cv2.imread(os.path.join(self.directory, self.filename))

    def draw_annotations(self):
        fontScale = 1.4
        font = cv2.FONT_HERSHEY_SIMPLEX
        lineType = 4

        im = self.ocv_load()
        colors = {'Ringed Seal': (70, 153, 144), # Teal
                  'Bearded Seal': (67, 99, 216), #blue
                  'Polar Bear': (240, 50, 230), #magenta
                  'UNK Seal':  (255, 225, 25) #yellow
                 }
        for annotation in self.annotations:
            box = annotation.eo_box
            color = colors.get(annotation.species.name)
            if color is None: color = (230, 25, 75) #red

            cv2.rectangle(im, (box.x1, box.y1), (box.x2, box.y2), color, 4)
            cv2.putText(im, annotation.species.name, (box.x1, box.y1), font,
                        fontScale, color, lineType)

        return im

class IRImage(SurveyBase):
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
    
    def to_dict(self):
        res = {'w': self.width,
               'h': self.height,
               'c': self.depth,
               'filename': self.filename,
               'directory': self.directory,
               'event_key': self.event_key
               }
        return res

    def path(self):
        return os.path.join(self.directory, self.filename)

    def ocv_load(self):
        return cv2.imread(os.path.join(self.directory, self.filename), cv2.IMREAD_UNCHANGED)

    def ocv_load_normed(self):
        im = self.ocv_load()
        im_norm = ((im - np.min(im)) / (0.0 + np.max(im) - np.min(im)))
        im_norm = im_norm * 255.0
        im_norm = im_norm.astype(np.uint8)
        return im_norm

DBImage = TypeVar('DBImage', EOImage, IRImage)

models = [Survey,Flight,Camera, HeaderMeta, InstrumentMeta, Homography, EOImage, IRImage]
__all__ = ["Survey","Flight","Camera", "HeaderMeta", "InstrumentMeta",
           "Homography", "EOImage", "IRImage", "DBImage", "models"]

