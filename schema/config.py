import os

config = {
    "db_password": os.environ["DB_PWD"],
    "db_user": os.environ["DB_USR"],
    "db_name": "noaa",
    "db_host": os.environ["DB_HOST"],
    "schema_name": "noaa_test" if os.environ["DEBUG"] else "noaa"
}