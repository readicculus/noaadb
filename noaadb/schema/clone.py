import os

from sqlalchemy import create_engine, MetaData, DDL
from sqlalchemy import Column, Integer, String, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Engine to the database to query the data from
# (postgresql)
from noaadb import DATABASE_URI
from noaadb.schema.models import TABLE_DEPENDENCY_ORDER, Base, Sighting, LabelEntry, IRLabelEntry, EOLabelEntry, \
    ImageDimension, Chip, LabelChipBase, LabelChips, FPChips, TrainTestSplit, NOAAImage, Job, Worker, Species
from noaadb.schema.schema_ops import drop_schema, create_schema
from scripts.util import printProgressBar

source_engine = create_engine(DATABASE_URI)
SourceSession = sessionmaker(bind=source_engine)
# Engine to the database to store the results in
# (sqlite)
REMOTE_DATABASE_URI = 'postgres+psycopg2://%s:%s@%s:5432/%s' % (os.environ["R_DB_USR"],
                                                         os.environ["R_DB_PWD"],
                                                         os.environ["R_DB_HOST"],
                                                         os.environ["R_DB_NAME"])

dest_engine = create_engine(REMOTE_DATABASE_URI)
DestSession = sessionmaker(dest_engine)

# drop dest
drop_schema(dest_engine, echo=True)
create_schema(dest_engine, echo=True)

# This is the query we want to persist in a new table:
sourceSession = SourceSession()
destSession = DestSession()
# query= sourceSession.query(Pet.name, Pet.race).filter_by(race='cat')

tables = TABLE_DEPENDENCY_ORDER

def copy_table(src_session, src_class, dst_session, dst_class):
    dest_columns = [col.name for col in dst_class().__table__.columns]
    r=src_session.query(src_class).all()
    count = 0
    total=len(r)
    print("Copying %s - %d records" % (src_class.__table__.fullname, total))
    missing_cols = set()
    for i in r:
        if count % 10 == 0:
            printProgressBar(count, total, prefix='Progress:', suffix='Complete', length=50)
        count += 1
        j=dst_class()
        for col in i.__table__.columns:
            if col.name in dest_columns:
                setattr(j, col.name, getattr(i, col.name))
            else:
                missing_cols.add(col)
        dst_session.add(j)
        if count % 1000 == 0:
            dst_session.commit()
    for a in missing_cols:
        print("%s not in dest %s" % (a,dst_class().__table__.name))

for table in  [NOAAImage,Job,Worker,Species,Sighting, LabelEntry, IRLabelEntry, EOLabelEntry, ImageDimension, Chip, LabelChipBase, LabelChips, FPChips, TrainTestSplit]:
    # disable_constrain = "ALTER TABLE {0} DISABLE TRIGGER ALL;".format(table.__table__.fullname)
    # enable_constrain = "ALTER TABLE {0} ENABLE TRIGGER ALL;".format(table.__table__.fullname)
    # dest_engine.execute("SET search_path TO %s" % os.environ["R_DB_NAME"])
    # dest_engine.execute(DDL(disable_constrain))
    dest_engine.execute(DDL("set session_replication_role = replica;"))
    copy_table(sourceSession, table, destSession, table)
    destSession.commit()
    # dest_engine.execute(DDL(enable_constrain))
    dest_engine.execute(DDL("set session_replication_role = DEFAULT;"))
