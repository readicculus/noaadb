from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from noaadb.schema.config import config

DATABASE_URI = 'postgres+psycopg2://%s:%s@%s:5432/%s' % (config["db_user"], config["db_password"], config["db_host"], config["db_name"])
# DATABASE_URI_REMOTE = 'postgres+psycopg2://%s:%s@yuvalboss.com:5432/noaa' % ("readonly", "readonly")
engine = create_engine(DATABASE_URI)
Session = sessionmaker(engine)