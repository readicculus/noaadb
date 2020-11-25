import collections
import logging
import os

import luigi.contrib.hdfs
import sqlalchemy
from luigi.contrib.sqla import SQLAlchemyTarget
from sqlalchemy import DDL

from noaadb import DATABASE_URI, engine
from noaadb.schema.models import *

class NOAADBTableTarget(luigi.Target):
    Connection = collections.namedtuple("Connection", "engine pid")

    def __init__(self, connection_string, table_model, echo=False):
        self.connection_string = connection_string
        self.connect_args = {}
        self.echo = echo
        self.table_model = table_model

    @property
    def engine(self):
        """
        Return an engine instance, creating it if it doesn't exist.

        Recreate the engine connection if it wasn't originally created
        by the current process.
        """
        pid = os.getpid()
        conn = SQLAlchemyTarget._engine_dict.get(self.connection_string)
        if not conn or conn.pid != pid:
            # create and reset connection
            engine = sqlalchemy.create_engine(
                self.connection_string,
                connect_args=self.connect_args,
                echo=self.echo
            )
            SQLAlchemyTarget._engine_dict[self.connection_string] = self.Connection(engine, pid)
        return SQLAlchemyTarget._engine_dict[self.connection_string].engine

    def touch(self):
        if not self.exists():
            # create schema if not exists
            schema = self.table_model.__table__.schema
            engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS %s" % schema))

            logger = logging.getLogger('luigi-interface')
            logger.info('NOAADBTableTarget: Creating %s' % self.table_model.__table__)
            self.table_model.__table__.create(engine)

    def remove(self):
        if self.exists():
            logger = logging.getLogger('luigi-interface')
            logger.info('NOAADBTableTarget: Removing %s' % self.table_model.__table__)
            self.table_model.__table__.drop(engine)

    def exists(self):

        exists = self.table_model.__table__.exists(engine)
        return exists


    def open(self, mode):
        raise NotImplementedError("Cannot open() NOAADBTableTarget")


class CreateTableTask(luigi.Task):
    children = luigi.ListParameter()

    def requires(self):
        if len(list(self.children)) > 1:
            tables = list(reversed(self.children))
            return CreateTableTask(children=list(reversed(tables[1:])))
        return None

    def output(self):
        tables = list(reversed(self.children))
        self.table_name = tables.pop(0)
        t = globals()[self.table_name]
        return NOAADBTableTarget(DATABASE_URI, t)


    def run(self):
        self.output().touch()
