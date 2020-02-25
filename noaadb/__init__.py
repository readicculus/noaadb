from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from noaadb.schema.config import config

DATABASE_URI = 'postgres+psycopg2://%s:%s@%s:5432/noaa' % (config["db_user"], config["db_password"], config["db_host"])
# DATABASE_URI_REMOTE = 'postgres+psycopg2://%s:%s@yuvalboss.com:5432/noaa' % ("readonly", "readonly")
noaa_engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=noaa_engine)
