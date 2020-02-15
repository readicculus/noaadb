from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from noaadb.config import config

DATABASE_URI = 'postgres+psycopg2://%s:%s@localhost:5432/noaa' % (config["db_user"], config["db_password"])
noaa_engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=noaa_engine)
