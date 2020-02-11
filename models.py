from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Date, VARCHAR, DateTime, BOOLEAN, ForeignKey, \
    MetaData, Integer
from sqlalchemy.orm import validates
from sqlalchemy.schema import CheckConstraint
from sqlalchemy.dialects.postgresql import ENUM
FILEPATH = VARCHAR(400)

Base = declarative_base(metadata=MetaData(schema="noaa_test"))

class Images(Base):
    __tablename__ = 'images'
    id = Column(Integer, autoincrement=True, primary_key=True)
    file_name = Column(VARCHAR(200), nullable=False, unique=True)
    file_path = Column(FILEPATH, nullable=False, unique=True)
    type = Column(ENUM("RGB", "IR", "UV", name="im_type_enum", create_type=True), nullable=False)
    foggy = Column(BOOLEAN)
    quality = Column(Integer)
    width = Column(Integer, nullable=False)
    height = Column(Integer, nullable=False)
    depth = Column(Integer, nullable=False)
    flight = Column(VARCHAR(50))
    time = Column(DateTime(timezone=True))

    def __repr__(self):
        return "<Images(id='{}', type='{}', foggy='{}', quality='{}', width='{}', height='{}', depth='{}', flight='{}', time='{}', file_name='{}')>" \
            .format(self.id, self.type, self.foggy, self.quality, self.width, self.height, self.depth, self.flight, self.time, self.file_name)


class Chips(Base):
    __tablename__ = 'chips'
    id = Column(Integer, autoincrement=True, primary_key=True)
    image_id = Column(Integer, ForeignKey("images.id"), nullable=False)
    file_path = Column(FILEPATH)
    relative_x1 = Column(Integer, nullable=False)
    relative_y1 = Column(Integer, nullable=False)
    relative_x2 = Column(Integer, nullable=False)
    relative_y2 = Column(Integer, nullable=False)

    def __repr__(self):
        return "<Chips(id='{}', path='{}', relative_x1='{}', relative_y1='{}', relative_x2='{}', relative_y2='{}', image_id='{}')>" \
            .format(self.id, self.file_path, self.relative_x1, self.relative_y1, self.relative_x2, self.relative_y2, self.image_id)


class Manifests(Base):
    __tablename__ = 'manifests'
    manifest_id = Column(Integer, primary_key=True)
    job_name = Column(VARCHAR(100), nullable=False)
    file_path = Column(FILEPATH, nullable=False)
    notes = Column(VARCHAR(500))

    def __repr__(self):
        return "<Manifests(manifest_id='{}', job_name='{}', notes='{}')>" \
            .format(self.manifest_id, self.job_name, self.file_path, self.notes)


class Workers(Base):
    __tablename__ = 'workers'
    worker_id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(VARCHAR(100), nullable=False)
    human = Column(BOOLEAN, nullable=False)

    def __repr__(self):
        return "<Workers(worker_id='{}', name='{}', human='{}')>" \
            .format(self.worker_id, self.name, self.human)


class Species(Base):
    __tablename__ = 'species'
    species_id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(VARCHAR(100), nullable=False)

    def __repr__(self):
        return "<Species(species_id='{}', name='{}')>" \
            .format(self.species_id, self.name)

class LabelEntry(Base):
    __tablename__ = 'label_entry'
    id = Column(Integer, autoincrement=True, primary_key=True)
    image = Column(Integer, nullable=False)
    species = Column(Integer, ForeignKey('species.species_id'), nullable=False)
    x1 = Column(Integer, nullable=False)
    x2 = Column(Integer, nullable=False)
    y1 = Column(Integer, nullable=False)
    y2 = Column(Integer, nullable=False)
    age_class = Column(VARCHAR(50))
    confidence = Column(Integer)
    is_shadow = Column(BOOLEAN, nullable=False)
    start_date = Column(Date)
    end_date = Column(Date)
    hotspot_id = Column(VARCHAR(50))
    worker = Column(Integer, ForeignKey('workers.worker_id'), nullable=False)
    manifest = Column(Integer, ForeignKey('manifests.manifest_id'), nullable=False)

    __table_args__ = (
        CheckConstraint('x1<x2 AND y1<y2',
                        name='bbox_valid'),
        )

    @validates('x1','x2','y1','y2')
    def validate_bbox(self, key, f) -> str:
        if key == 'y2' and self.y1 >= f:
            raise ValueError('y1 >= y2')
        if key == 'x2' and self.x1 >= f:
            raise ValueError('x1 >= x2')
        return f
    def __repr__(self):
        return "<LabelEntry(id='{}', image='{}', species='{}', x1='{}', x2='{}', y1='{}', y2='{}', age_class='{}', confidence='{}', is_shadow='{}', start_date='{}', end_date='{}', hotspot_id='{}', worker='{}', manifest='{}')>" \
            .format(self.id, self.image, self.species, self.x1, self.x2, self.y1, self.y2,
                    self.age_class, self.confidence, self.is_shadow, self.start_date, self.end_date, self.hotspot_id, self.worker, self.manifest)

class Labels(Base):
    __tablename__ = 'labels'
    id = Column(Integer, autoincrement=True, primary_key=True)
    eo_label = Column(Integer, ForeignKey('label_entry.id'))
    ir_label = Column(Integer, ForeignKey('label_entry.id'))
    hs_id = Column(VARCHAR(50))
    eo_accepted = Column(BOOLEAN, default=False)
    ir_accepted = Column(BOOLEAN, default=False)

    def __repr__(self):
        return "<Labels(id='{}', eo_label='{}', ir_label='{}', hs_id='{}', eo_finalized='{}', ir_finalized='{}')>" \
            .format(self.id, self.eo_label, self.ir_label, self.hs_id, self.eo_accepted, self.ir_accepted)



