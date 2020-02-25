from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from noaadb import config, DATABASE_URI

engine=None
session=None

def connection():
    pass
def create():
    engine = create_engine(DATABASE_URI, echo=True)
    session = Session()