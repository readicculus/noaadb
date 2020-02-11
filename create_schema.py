from sqlalchemy import create_engine

from noaadb import DATABASE_URI
from noaadb.models import Base

engine = create_engine(DATABASE_URI, echo=True)

Base.metadata.create_all(engine)

print("Success")
