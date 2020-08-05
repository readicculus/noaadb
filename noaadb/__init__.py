from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def check_env_variables():
    required_env_variables = ['DB_NAME','DB_PWD', 'DB_USR', 'DB_HOST']
    missing_end_variables = []
    for v in required_env_variables:
        if not v in os.environ:
            missing_end_variables.append(v)

    if len(missing_end_variables):
        raise Exception('Missing environment variables %s' % ', '.join(missing_end_variables))

def get_config():
    config = None
    is_remote = int("REMOTE" in os.environ and os.environ["REMOTE"])
    is_debug = int(not "DEBUG" in os.environ or os.environ["DEBUG"])
    if is_debug or (not is_debug and is_remote):
        config = {
            "db_password": os.environ["DB_PWD"] if "DB_PWD" in os.environ else None,
            "db_user": os.environ["DB_USR"] if "DB_USR" in os.environ else None,
            "db_name": os.environ["DB_NAME"],
            "db_host": os.environ["DB_HOST"] if "DB_HOST" in os.environ else None
        }
    return config

check_env_variables()

config = get_config()
DATABASE_URI = 'postgres+psycopg2://%s:%s@%s:5432/%s' % \
               (config["db_user"], config["db_password"], config["db_host"], config["db_name"])
print(DATABASE_URI)
engine = create_engine(DATABASE_URI)
Session = sessionmaker(engine)
