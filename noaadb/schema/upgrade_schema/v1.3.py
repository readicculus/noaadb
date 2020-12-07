from sqlalchemy import create_engine, DDL

from noaadb import DATABASE_URI
from noaadb.schema.models import NUC

engine = create_engine(DATABASE_URI, echo=False)

engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS ml_data"))


NUC.__table__.drop(bind=engine, checkfirst=True)
NUC.__table__.create(bind=engine)