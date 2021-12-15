from sqlalchemy import DDL

from noaadb.schema.models.archive.label_data import LabelBase
from noaadb.schema.models.survey_data import SurveyBase
from noaadb.schema.models.archive.utilities import UtilitiesBase


def drop_survey_schema(engine,tables_only=True):
    if not tables_only:
        engine.execute(DDL("DROP SCHEMA IF EXISTS survey_data CASCADE"))
    SurveyBase.metadata.drop_all(engine)
    print("Dropped survey schema")

def drop_label_schema(engine,tables_only=True):
    if not tables_only:
        engine.execute(DDL("DROP SCHEMA IF EXISTS label_data CASCADE"))
    LabelBase.metadata.drop_all(engine)
    print("Dropped label schema")


def drop_utilities_schema(engine,tables_only=True):
    if not tables_only:
        engine.execute(DDL("DROP SCHEMA IF EXISTS utilities CASCADE"))
    UtilitiesBase.metadata.drop_all(engine)
    print("Dropped utilities schema")

def create_survey_schema(engine,tables_only=True):
    if not tables_only:
        engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS survey_data"))
    SurveyBase.metadata.create_all(engine, checkfirst=False)
    print("Success")

def create_label_schema(engine,tables_only=True):
    if not tables_only:
        engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS label_data"))
    LabelBase.metadata.create_all(engine, checkfirst=False)


def create_utilities_schema(engine,tables_only=True):
    if not tables_only:
        engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS utilities"))
    UtilitiesBase.metadata.create_all(engine, checkfirst=False)