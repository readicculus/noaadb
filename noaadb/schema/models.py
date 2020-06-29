import enum
from operator import and_

from sqlalchemy.ext.declarative import declarative_base, declared_attr, ConcreteBase, AbstractConcreteBase
from sqlalchemy import Column, Date, VARCHAR, DateTime, BOOLEAN, ForeignKey, \
    MetaData, Integer, UniqueConstraint, Float, JSON, func, event, select, Unicode, String, BigInteger, \
    ForeignKeyConstraint
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import validates, relationship, column_property, backref, aliased, with_polymorphic
from sqlalchemy.schema import CheckConstraint, Sequence
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.orm.session import Session
class ImageType(enum.IntEnum):
    RGB = 1
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
FILENAME = VARCHAR(200)
FILEPATH = VARCHAR(400)
meta = MetaData()
Base = declarative_base(metadata=meta)
class FlightCamId(Base): # TODO read https://avacariu.me/writing/2019/composite-foreign-keys-and-many-to-many-relationships-in-sqlalchemy
    __tablename__ = 'flight_cam_meta'
    flight = Column(String(20), primary_key=True)
    cam = Column(String(20), primary_key=True)
    survey = Column(String(20), primary_key=True)
    __table_args__ = (
        {'schema': "noaa_surveys"}
    )


class FlightMetaHeader(Base):
    __tablename__ = 'header_meta'
    stamp = Column(BigInteger, primary_key=True)
    frame_id = Column(VARCHAR(10))
    seq = Column(Integer)
    flight = Column(String(20), nullable=False)
    cam = Column(String(20), nullable=False)
    survey = Column(String(20), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint([flight, cam, survey], [FlightCamId.flight, FlightCamId.cam, FlightCamId.survey],
                             onupdate="CASCADE", ondelete="CASCADE"),
        {'schema': "flight_meta"}
    )

class FlightMetaInstruments(Base):
    __tablename__ = 'ins_meta'
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
    header_id = Column(BigInteger, ForeignKey("flight_meta.header_meta.stamp"))
    meta_header = relationship("FlightMetaHeader")
    flight = Column(String(20), nullable=False)
    cam = Column(String(20), nullable=False)
    survey = Column(String(20), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint([flight, cam, survey], [FlightCamId.flight, FlightCamId.cam, FlightCamId.survey],
                             onupdate="CASCADE", ondelete="CASCADE"),
        {'schema': "flight_meta"}
    )



class FlightMetaEvent(Base):
    __tablename__ = 'evt_meta'
    id = Column(Integer, autoincrement=True, primary_key=True)
    event_port = Column(Integer)
    event_num = Column(Integer)
    time = Column(Float)
    header_id = Column(BigInteger, ForeignKey("flight_meta.header_meta.stamp"))
    meta_header = relationship("FlightMetaHeader")
    flight = Column(String(20), nullable=False)
    cam = Column(String(20), nullable=False)
    survey = Column(String(20), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint([flight, cam, survey], [FlightCamId.flight, FlightCamId.cam, FlightCamId.survey],
                             onupdate="CASCADE", ondelete="CASCADE"),
        {'schema': "flight_meta"}
    )

    meta_header = relationship("FlightMetaHeader")


class NOAAImage(Base):
    __tablename__ = 'images'
    file_name = Column(FILEPATH, primary_key=True)
    file_path = Column(FILEPATH, nullable=False)
    type = Column(ENUM(ImageType, name="im_type_enum", metadata=meta, schema="noaa_surveys", create_type=True), nullable=False)
    foggy = Column(BOOLEAN)
    quality = Column(Integer)
    width = Column(Integer, nullable=False)
    height = Column(Integer, nullable=False)
    depth = Column(Integer, nullable=False)
    timestamp = Column(DateTime(timezone=True))

    is_bigendian = Column(BOOLEAN)
    step = Column(Integer)
    encoding = Column(VARCHAR(20))
    header_id = Column(BigInteger, ForeignKey("flight_meta.header_meta.stamp"))
    meta_header = relationship("FlightMetaHeader")
    ins_id = Column(Integer, ForeignKey("flight_meta.ins_meta.id"))
    meta_instrument = relationship("FlightMetaInstruments")
    evt_id = Column(Integer, ForeignKey("flight_meta.evt_meta.id"))
    meta_evt = relationship("FlightMetaEvent")

    flight = Column(String(20), nullable=False)
    cam = Column(String(20), nullable=False)
    survey = Column(String(20), nullable=False)
    __table_args__ = (
        ForeignKeyConstraint([flight, cam, survey], [FlightCamId.flight, FlightCamId.cam, FlightCamId.survey],
                             onupdate="CASCADE", ondelete="CASCADE"),
        {'schema': "noaa_surveys"}
    )

    # image_dimension_id = Column(Integer, ForeignKey("chips.image_dimensions.id"), nullable=False)
    # image_dimension = relationship("ImageDimensions")
    __mapper_args__ = {'polymorphic_on': type}

    def __repr__(self):
        return "<NOAAImage(name='{}', type='{}', foggy='{}', quality='{}', width='{}', height='{}', depth='{}', flight='{}', time='{}','" \
               " survey='{}', timestamp='{}', cam_position='{}')>" \
            .format(self.file_name, self.type, self.foggy, self.quality, self.width, self.height, self.depth, self.flight, self.timestamp
                    , self.survey, self.timestamp, self.cam_position)

class EOImage(NOAAImage):
    __mapper_args__ = {
        'polymorphic_identity':ImageType.RGB,
    }
class IRImage(NOAAImage):
    __mapper_args__ = {
        'polymorphic_identity':ImageType.IR,
    }


class Job(Base):
    __tablename__ = 'jobs'
    __table_args__ = {'schema': "noaa_surveys"}
    name = Column(JOBWORKERNAME, nullable=False, unique=True, primary_key=True)
    file_path = Column(FILEPATH, nullable=False)
    notes = Column(VARCHAR(500))

    def __repr__(self):
        return "<Job(id='{}', job_name='{}', notes='{}')>" \
            .format(self.name, self.file_path, self.notes)


class Worker(Base):
    __tablename__ = 'workers'
    __table_args__ = {'schema': "noaa_surveys"}
    name = Column(JOBWORKERNAME, nullable=False, unique=True, primary_key=True)
    human = Column(BOOLEAN, nullable=False)

    def __repr__(self):
        return "<Worker(name='{}', human='{}')>" \
            .format(self.name, self.human)


class Species(Base):
    __tablename__ = 'species'
    __table_args__ = {'schema': "noaa_surveys"}
    id = Column(Integer,
                Sequence('species_seq', start=1, increment=1, metadata=meta, schema="noaa_surveys"),
                primary_key=True)
    name = Column(VARCHAR(100), nullable=False, unique=True)

    def __repr__(self):
        return "<Species(id='{}', name='{}')>" \
            .format(self.id, self.name)

class Sighting(Base):
    __tablename__ = 'sightings'
    id = Column(Integer,
                Sequence('sightings_seq', start=1, increment=1, metadata=meta, schema="noaa_surveys"),
                primary_key=True)

    hotspot_id = Column(VARCHAR(50))
    species_id = Column(Integer, ForeignKey('noaa_surveys.species.id'), nullable=False)
    species = relationship("Species")
    age_class = Column(VARCHAR(50))
    ir_label_id = Column(Integer, ForeignKey('noaa_surveys.ir_label.id'), nullable=True)
    eo_label_id = Column(Integer, ForeignKey('noaa_surveys.eo_label.id'), nullable=True)

    ir_label = relationship("IRLabelEntry", foreign_keys=[ir_label_id], backref=backref('sightings', cascade='all,delete'))
    eo_label = relationship("EOLabelEntry", foreign_keys=[eo_label_id], backref=backref('sightings', cascade='all,delete'))

    __table_args__ = ({'schema': "noaa_surveys"})

    discriminator = Column('label_type', ENUM(LabelType, name="label_type_enum", metadata=meta, schema="noaa_surveys", create_type=True))
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




class LabelEntry(ConcreteBase, Base):
    # __abstract__ = True
    __tablename__ = 'labels'
    # id = Column(Integer,
    #             Sequence('label_seq', start=1, increment=1, metadata=meta, schema="noaa_surveys"),
    #             primary_key=True)
    id = Column(Integer, primary_key=True)
    # @declared_attr
    # def id(cls):
    #     return Column(Integer, primary_key=True,autoincrement=True)

    @declared_attr
    def image_id(cls):
        return Column(FILENAME, ForeignKey('noaa_surveys.images.file_name'), nullable=False)

    @declared_attr
    def image(cls):
        return relationship("NOAAImage")

    @declared_attr
    def job_id(cls):
        return Column(JOBWORKERNAME, ForeignKey('noaa_surveys.jobs.name'), nullable=False)
    @declared_attr
    def job(cls):
        return relationship("Job")

    @declared_attr
    def worker_id(cls):
        return Column(JOBWORKERNAME, ForeignKey('noaa_surveys.workers.name'), nullable=False)

    @declared_attr
    def worker(cls):
        return relationship("Worker")

    @declared_attr
    def species_id(cls):
        return Column(Integer, ForeignKey('noaa_surveys.species.id'), nullable=False)

    @declared_attr
    def species(cls):
        return relationship("Species")
    # @declared_attr
    # def sighting_id(cls):
    #     return Column(Integer, ForeignKey('noaa_surveys.sightings.id'), nullable=False)
    #
    # @declared_attr
    # def sighting(cls):
    #     return relationship("Sighting",back_populates="labels")

    # sighting = relationship("Sighting",back_populates="labels", uselist=False)
    x1 = Column(Integer)
    x2 = Column(Integer)
    y1 = Column(Integer)
    y2 = Column(Integer)

    confidence = Column(Float)
    is_shadow = Column(BOOLEAN, nullable=True)
    start_date = Column(Date)
    end_date = Column(Date)

    # @property
    # def image_type(self):
    #     if self.image is None:
    #         return None
    #     return self.image.type

    __table_args__ = (
        CheckConstraint('x1<=x2 AND y1<=y2',
                        name='bbox_valid'),
        UniqueConstraint('confidence', 'x1', 'x2', 'y1', 'y2', 'image_id', 'species_id', name='_label_unique_constraint'),
        {'schema': "noaa_surveys"}
        )


    discriminator = Column('label_type', ENUM(ImageType, name="im_type_enum", metadata=meta, schema="noaa_surveys", create_type=True))
    __mapper_args__ = {'polymorphic_on': discriminator,
                       'polymorphic_identity':ImageType.ALL,
                       # 'with_polymorphic': '*'
                       }


    @validates('x1', 'x2', 'y1', 'y2')
    def validate_bbox(self, key, f) -> str:
        if key == 'y2' and self.y2 is not None and self.y1 > f:
            raise ValueError('y1 > y2')
        if key == 'x2' and self.x2 is not None and self.x1 > f:
            raise ValueError('x1 > x2')
        return f
    def __repr__(self):
        return "<Label(id='{}', image_id='{}', x1='{}', x2='{}'," \
               " y1='{}', y2='{}', age_class='{}', confidence='{}', is_shadow='{}', start_date='{}'," \
               " end_date='{}', worker_id='{}', manifest='{}')>" \
            .format(self.id, self.image_id, self.x1, self.x2,
                    self.y1, self.y2, self.age_class, self.confidence, self.is_shadow,
                    self.start_date, self.end_date, self.worker_id, self.job_id)

class IRLabelEntry(LabelEntry):
    row_id = Column(Integer, primary_key=True,autoincrement=True)
    id = Column(Integer, ForeignKey('noaa_surveys.labels.id', ondelete="CASCADE"), unique=True)

    __tablename__ = 'ir_label'
    __mapper_args__ = {
    'polymorphic_identity':ImageType.IR,
        'polymorphic_load': 'inline',
        'inherit_condition': id == LabelEntry.id,
                       'with_polymorphic': '*'
    }

    __table_args__ = (
        {'schema': "noaa_surveys"}
        )
    # root = relationship(
    #     'LabelEntry',
    #     backref='ir_label', primaryjoin=LabelEntry.id == id, remote_side=LabelEntry.id)
class EOLabelEntry(LabelEntry):
    row_id = Column(Integer, primary_key=True,autoincrement=True)
    id = Column(Integer, ForeignKey('noaa_surveys.labels.id', ondelete="CASCADE"),unique=True)

    __tablename__ = 'eo_label'
    __mapper_args__ = {
    'polymorphic_identity':ImageType.RGB,
        'polymorphic_load': 'inline',
        'inherit_condition': id == LabelEntry.id,
        'with_polymorphic': '*'
    }
    __table_args__ = (
        {'schema': "noaa_surveys"}
        )
    # root = relationship(
    #     'LabelEntry',
    #     backref='eo_label', primaryjoin=LabelEntry.id == id, remote_side=LabelEntry.id)
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


class LabelChipBase(Base):
    __abstract__=True
    __table_args__ = {'schema': "chips"}

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    @declared_attr
    def chip_id(cls):
        return Column(Integer, ForeignKey("chips.chip.id"), nullable=False)

    @declared_attr
    def chip(cls):
        return relationship("Chip")

    @declared_attr
    def label_id(cls):
        return Column(Integer, ForeignKey("noaa_surveys.eo_label.id"), nullable=False)

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
    __table_args__ = {'schema': "chips"}
    id = Column(Integer,
                Sequence('chip_hs_seq', start=1, increment=1, metadata=meta, schema="chips"),
                primary_key=True)

    def __repr__(self):
        return "<ChipHotspot(id='{}', relative_x1='{}', relative_x2='{}', relative_y1='{}', relative_y2='{}', chip_id='{}', label_id='{}', percent_intersection='{}')>" \
            .format(self.id, self.relative_x1, self.relative_x2, self.relative_y1, self.relative_y2, self.chip_id, self.label_id, self.percent_intersection)

class FPChips(LabelChipBase):
    __tablename__ = 'fp_chips'
    __table_args__ = {'schema': "chips"}
    id = Column(Integer,
                Sequence('fp_chips_seq', start=1, increment=1, metadata=meta, schema="chips"),
                primary_key=True)


class TrainTestSplit(Base):
    __tablename__ = 'train_test_split'
    __table_args__ = {'schema': "ml"}
    id = Column(Integer,
                Sequence('train_test_seq', start=1, increment=1, metadata=meta, schema="ml"),
                primary_key=True)
    label_id = Column(Integer, ForeignKey("noaa_surveys.labels.id"), nullable=False)
    label = relationship('LabelEntry', foreign_keys=[label_id])
    type = Column(ENUM(MLType, name="ml_type_enum", metadata=meta, schema="ml", create_type=True), nullable=False)

    def __repr__(self):
        return "<TrainTestSplit(id='{}', label='{}', type='{}')>" \
            .format(self.id, self.label, self.type)


TABLE_DEPENDENCY_ORDER = [FlightMetaHeader,FlightMetaEvent, FlightMetaInstruments, NOAAImage,Job,Worker,Species,Sighting, LabelEntry, IRLabelEntry, EOLabelEntry, ImageDimension, Chip, LabelChipBase, LabelChips, FPChips, TrainTestSplit]