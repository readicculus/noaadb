from sqlalchemy import create_engine
from noaadb.models import Base
from noaadb.config import config

DATABASE_URI = 'postgres+psycopg2://%s:%s@localhost:5432/noaa' % (config["db_user"], config["db_password"])
engine = create_engine(DATABASE_URI, echo=True)

Base.metadata.create_all(engine)

print("Success")
