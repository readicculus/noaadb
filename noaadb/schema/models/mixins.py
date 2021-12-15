import datetime

import sqlalchemy as sa
from sqlalchemy import event

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

