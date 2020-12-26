from sqlalchemy import Column, Integer, ForeignKey, Date

from noaadb.schema import FILENAME
from noaadb.schema.models import Base, EOImage, IRImage, CreatedUpdatedTimeMixin

schema_name = 'manual_review'
# label_meta = MetaData(schema=schema_name)
# DetectionBase = declarative_base(metadata=label_meta)
ManualReviewBase = Base

class IRWithoutErrors(CreatedUpdatedTimeMixin, ManualReviewBase):
    __tablename__ = 'ir_without_errors'
    id = Column(Integer, autoincrement=True, primary_key=True)
    ir_event_key = Column(FILENAME, ForeignKey(IRImage.event_key), nullable=False, unique=True)
    __table_args__ = (
        {'schema': schema_name},
    )

    def __init__(self, **kwargs):
        super(IRWithoutErrors, self).__init__(**kwargs)
        # do custom initialization here
        self.register()


class IRWithErrors(CreatedUpdatedTimeMixin, ManualReviewBase):
    __tablename__ = 'ir_with_error'
    id = Column(Integer, autoincrement=True, primary_key=True)
    ir_event_key = Column(FILENAME, ForeignKey(IRImage.event_key), nullable=False, unique=True)

    __table_args__ = (
        {'schema': schema_name},
    )
    def __init__(self, **kwargs):
        super(IRWithErrors, self).__init__(**kwargs)
        # do custom initialization here
        self.register()

class IRVerifiedBackground(CreatedUpdatedTimeMixin, ManualReviewBase):
    __tablename__ = 'ir_verified_background'
    id = Column(Integer, autoincrement=True, primary_key=True)
    ir_event_key = Column(FILENAME, ForeignKey(IRImage.event_key), nullable=False, unique=True)
    __table_args__ = (
        {'schema': schema_name},
    )

    def __init__(self, **kwargs):
        super(IRVerifiedBackground, self).__init__(**kwargs)
        # do custom initialization here
        self.register()

class IRUncertainBackground(CreatedUpdatedTimeMixin, ManualReviewBase):
    __tablename__ = 'ir_uncertain_background'
    id = Column(Integer, autoincrement=True, primary_key=True)
    ir_event_key = Column(FILENAME, ForeignKey(IRImage.event_key), nullable=False, unique=True)
    __table_args__ = (
        {'schema': schema_name},
    )

    def __init__(self, **kwargs):
        super(IRUncertainBackground, self).__init__(**kwargs)
        # do custom initialization here
        self.register()