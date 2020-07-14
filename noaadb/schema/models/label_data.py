import enum

from sqlalchemy import Column, Date, VARCHAR, BOOLEAN, ForeignKey, \
    MetaData, Integer, Float
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.ext.declarative import declarative_base, declared_attr, ConcreteBase
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import validates, relationship
from sqlalchemy.schema import CheckConstraint, Sequence

from noaadb.schema import JOBWORKERNAME, FILEPATH, FILENAME
from noaadb.schema.models import IRImage, EOImage
from noaadb.schema.models.survey_data import ImageType, FusedImage

schema_name = 'label_data'
label_meta = MetaData(schema=schema_name)
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
    __tablename__ = 'labels'

    id = Column(Integer, autoincrement=True, primary_key=True)

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
    hotspot_id = Column(VARCHAR(50))
    age_class = Column(VARCHAR(50))

    confidence = Column(Float)
    is_shadow = Column(BOOLEAN, nullable=True)
    start_date = Column(Date)
    end_date = Column(Date)

    __table_args__ = (
        CheckConstraint('x1<=x2 AND y1<=y2',
                        name='bbox_valid'),
        )

    image_type = Column('label_type', ENUM(ImageType, name="im_type_enum", metadata=label_meta, create_type=False))
    __mapper_args__ = {'polymorphic_on': image_type
                       }

    def to_dict(cls):
        res = {
            'image_id': cls.image_id,
            'x1': cls.x1,
            'x2': cls.x2,
            'y1': cls.y1,
            'y2': cls.y2,
            'conf': cls.confidence,
            'species': cls.species.name,
            'age_class': cls.age_class,
            'hotspot_id': cls.hotspot_id,
            'job': cls.job.name,
            'worker': cls.worker.name
        }
        return res
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


class EOIRLabelPair(LabelBase):
    __tablename__ = 'eoir_label_pair'
    id = Column(Integer,
                Sequence('sightings_seq', start=1, increment=1, metadata=label_meta),
                primary_key=True)

    ir_label_id = Column(Integer, ForeignKey(IRLabelEntry.id,ondelete="CASCADE"))
    eo_label_id = Column(Integer, ForeignKey(EOLabelEntry.id,ondelete="CASCADE"))

    ir_label = relationship("IRLabelEntry")
    eo_label = relationship("EOLabelEntry")

    def to_dict(cls):
        ir =  cls.ir_label.to_dict() if cls.ir_label else {}
        eo = cls.eo_label.to_dict() if cls.eo_label else {}
        shared_label = ir if cls.ir_label else eo
        res = {
            'eo_image': eo.get('image_id'),
            'ir_image': ir.get('image_id'),
            'species': shared_label.get('species'),
            'age_class': shared_label.get('age_class'),
            'hotspot_id': shared_label.get('hotspot_id'),
            'job': shared_label.get('job'),
            'worker': shared_label.get('worker'),
            'ir_label': None if cls.ir_label is None else {'x1': ir.get('x1'), 'x2': ir.get('x2'), 'y1': ir.get('y1'), 'y2': ir.get('y2'), 'conf': ir.get('conf')},
            'eo_label': None if cls.eo_label is None else {'x1': eo.get('x1'), 'x2': eo.get('x2'), 'y1': eo.get('y1'), 'y2': eo.get('y2'), 'conf': eo.get('conf')}
        }
        return res
# class FalsePositiveSightings(EOIRLabelPair):
#     __mapper_args__ = {
#         'polymorphic_identity':LabelType.FP,
#     }
# class TruePositiveEOIRLabelPair(EOIRLabelPair):
#     __mapper_args__ = {
#         'polymorphic_identity':LabelType.TP,
#     }
