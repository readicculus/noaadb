from noaadb.schema.utils.schema_ops import *

source_engine = create_engine(DATABASE_URI)
drop_ml_schema(source_engine, tables_only=False)
drop_label_schema(source_engine, tables_only=False)
drop_survey_schema(source_engine, tables_only=False)
create_survey_schema(source_engine, tables_only=False)
# create_label_schema(source_engine, tables_only=False)
# create_ml_schema(source_engine, tables_only=False)