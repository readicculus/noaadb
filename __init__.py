from sqlalchemy.orm import sessionmaker
from noaadb.config import config

Session = sessionmaker()
DATABASE_URI = 'postgres+psycopg2://%s:%s@localhost:5432/noaa' % (config["db_user"], config["db_password"])
