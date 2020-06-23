from sqlalchemy import create_engine

from noaadb import DATABASE_URI
from noaadb.schema.schema_ops import create_schema, drop_schema
source_engine = create_engine(DATABASE_URI)
drop_schema(source_engine, echo=True)
create_schema(source_engine, echo=True)