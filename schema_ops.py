from sqlalchemy import create_engine

from noaadb import DATABASE_URI
from noaadb.models import Base

def drop_schema(echo=False):
    engine = create_engine(DATABASE_URI, echo=echo)
    Base.metadata.drop_all(engine)

    print("Success")

def create_schema(echo=False):
    engine = create_engine(DATABASE_URI, echo=echo)

    Base.metadata.create_all(engine)

    print("Success")

def refresh_schema():
    drop_schema()
    create_schema()