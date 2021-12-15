from sqlalchemy import Column, VARCHAR, ForeignKey, \
    MetaData, Integer, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from noaadb.schema import FILEPATH
from noaadb.schema.models import Camera

schema_name = 'utilities'
utils_meta = MetaData(schema=schema_name)
UtilitiesBase = declarative_base(metadata=utils_meta)

class ImagePaths(UtilitiesBase):
    __tablename__ = 'image_paths'
    id = Column(Integer, autoincrement=True, primary_key=True)

    cam_id = Column(Integer, ForeignKey(Camera.id))
    cam = relationship(Camera)

    device = Column(VARCHAR(50)) # name of machine
    path = Column(FILEPATH) # path to images for specified cam on machine

    __table_args__ = (
        UniqueConstraint(cam_id,device,
                         name='device_cam_constraint'),
    )