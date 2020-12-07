from sqlalchemy import DDL

from noaadb.schema.models.archive.label_data import LabelBase
from noaadb.schema.models import MLBase
from noaadb.schema.models.survey_data import SurveyDataBase
from noaadb.schema.models.archive.utilities import UtilitiesBase


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
        engine.execute(DDL("DROP SCHEMA IF EXISTS ml_data CASCADE"))
    MLBase.metadata.drop_all(engine)
    print("Dropped ml_data schema")

def drop_utilities_schema(engine,tables_only=True):
    if not tables_only:
        engine.execute(DDL("DROP SCHEMA IF EXISTS utilities CASCADE"))
    UtilitiesBase.metadata.drop_all(engine)
    print("Dropped utilities schema")

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
    pass
    if not tables_only:
        engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS ml_data"))
    MLBase.metadata.create_all(engine, checkfirst=False)

def create_utilities_schema(engine,tables_only=True):
    if not tables_only:
        engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS utilities"))
    UtilitiesBase.metadata.create_all(engine, checkfirst=False)