from sqlalchemy import create_engine

from noaadb import DATABASE_URI
from noaadb.models import Base
from noaadb.config import config

engine = create_engine(DATABASE_URI, echo=True)
Base.metadata.drop_all(engine)

print("Success")
