from sqlalchemy import create_engine, DDL

from noaadb import DATABASE_URI
from noaadb.schema.models import MLBase, LabelBase, SurveyDataBase

def drop_survey_schema(engine,tables_only=True):
    if not tables_only:
        engine.execute(DDL("DROP SCHEMA IF EXISTS survey_data CASCADE"))
    SurveyDataBase.metadata.drop_all(engine)
    print("Dropped survey schema")

def drop_label_schema(engine,tables_only=True):
    if not tables_only:
        engine.execute(DDL("DROP SCHEMA IF EXISTS label_data CASCADE"))
    LabelBase.metadata.drop_all(engine)
    print("Dropped label schema")

def drop_ml_schema(engine,tables_only=True):
    if not tables_only:
        engine.execute(DDL("DROP SCHEMA IF EXISTS ml CASCADE"))
    MLBase.metadata.drop_all(engine)
    print("Dropped ml schema")

def create_survey_schema(engine,tables_only=True):
    if not tables_only:
        engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS survey_data"))
    SurveyDataBase.metadata.create_all(engine, checkfirst=False)
    print("Success")

def create_label_schema(engine,tables_only=True):
    if not tables_only:
        engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS label_data"))
    LabelBase.metadata.create_all(engine, checkfirst=False)

def create_ml_schema(engine,tables_only=True):
    if not tables_only:
        engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS ml"))
    MLBase.metadata.create_all(engine, checkfirst=False)

