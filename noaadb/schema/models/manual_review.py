from sqlalchemy import Column, Integer, ForeignKey

from noaadb.schema import FILENAME
from noaadb.schema.models import NDB_Base
from noaadb.schema.models.survey_data import IRImage
import datetime

import sqlalchemy as sa
from sqlalchemy import event

schema_name = 'manual_review'
ManualReviewBase = NDB_Base


class CreatedUpdatedTimeMixin(object):
  created_at = sa.Column('created_at', sa.DateTime, nullable=False)
  updated_at = sa.Column('updated_at', sa.DateTime, nullable=False)

  @staticmethod
  def create_time(mapper, connection, instance):
     now = datetime.datetime.utcnow()
     instance.created_at = now
     instance.updated_at = now

  @staticmethod
  def update_time(mapper, connection, instance):
     now = datetime.datetime.utcnow()
     instance.updated_at = now

  @classmethod
  def register(cls):
     sa.event.listen(cls, 'before_insert', cls.create_time)
     sa.event.listen(cls, 'before_update', cls.update_time)


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


models = [IRWithoutErrors, IRWithErrors, IRVerifiedBackground, IRUncertainBackground]
__all__ = ["IRWithoutErrors", "IRWithErrors", "IRVerifiedBackground", "IRUncertainBackground", "models"]
