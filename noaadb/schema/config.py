import os

config = {
    "db_password": os.environ["DB_PWD"] if "DB_PWD" in os.environ else None,
    "db_user": os.environ["DB_USR"] if "DB_USR" in os.environ else None,
    "db_name": "noaa_test" if "DEBUG" in os.environ and int(os.environ["DEBUG"]) else "noaa",
    "db_host": os.environ["DB_HOST"] if "DB_HOST" in os.environ else None,
    "schema_name": "noaa_labels"
}
