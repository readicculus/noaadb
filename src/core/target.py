import sqlalchemy
from luigi.contrib.sqla import SQLAlchemyTarget

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

