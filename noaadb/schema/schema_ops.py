from sqlalchemy import create_engine, DDL

from noaadb import DATABASE_URI
from noaadb.schema.models import Base

def drop_schema(echo=False):
    engine = create_engine(DATABASE_URI, echo=echo)
    Base.metadata.drop_all(engine)
    print("Success")


def create_schema(echo=False):
    engine = create_engine(DATABASE_URI, echo=echo)
    engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS noaa_surveys"))
    engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS chips"))
    engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS ml"))
    Base.metadata.create_all(engine)
    print("Success")

def refresh_schema():
    drop_schema()
    create_schema()