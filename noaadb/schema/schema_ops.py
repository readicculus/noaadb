from sqlalchemy import create_engine, DDL

from noaadb import DATABASE_URI
from noaadb.schema.models import Base

def drop_schema(engine,echo=False):
    # engine = create_engine(DATABASE_URI, echo=echo)
    engine.execute(DDL("DROP SCHEMA IF EXISTS noaa_surveys CASCADE"))
    engine.execute(DDL("DROP SCHEMA IF EXISTS chips CASCADE"))
    engine.execute(DDL("DROP SCHEMA IF EXISTS ml CASCADE"))
    engine.execute(DDL("DROP SCHEMA IF EXISTS flight_meta CASCADE"))
    Base.metadata.drop_all(engine)
    print("Success")


def create_schema(engine, echo=False):
    # engine = create_engine(DATABASE_URI, echo=echo)
    engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS noaa_surveys"))
    engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS chips"))
    engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS ml"))
    engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS flight_meta"))
    Base.metadata.create_all(engine, checkfirst=False)
    print("Success")

def refresh_schema():
    drop_schema()
    create_schema()