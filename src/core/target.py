import collections
import logging
import os
from noaadb import engine

import luigi.contrib.hdfs
import sqlalchemy
from luigi.contrib.sqla import SQLAlchemyTarget
from sqlalchemy import DDL


class SQLAlchemyCustomTarget(SQLAlchemyTarget):
    def remove(self):
        if self.marker_table_bound is None:
            self.create_marker_table()

        table = self.marker_table_bound
        id_exists = self.exists()
        with self.engine.begin() as conn:
            if id_exists:
                ins = table.delete().where(sqlalchemy.and_(table.c.update_id == self.update_id,
                                                           table.c.target_table == self.target_table))
                conn.execute(ins)

        assert not self.exists()


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
        logger = logging.getLogger('luigi-interface')
        if self.exists():
            logger.info('NOAADBTableTarget: ALREADY EXISTS %s' % self.table_model.__table__)
        else:
            # create schema if not exists
            schema = self.table_model.__table__.schema
            engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS %s" % schema))

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

