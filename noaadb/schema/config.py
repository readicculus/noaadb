import os
config = None
is_remote =  int("REMOTE" in os.environ and os.environ["REMOTE"])
is_debug = int(not "DEBUG" in os.environ or os.environ["DEBUG"])
if is_debug or (not is_debug and is_remote):
    config = {
        "db_password": os.environ["DB_PWD"] if "DB_PWD" in os.environ else None,
        "db_user": os.environ["DB_USR"] if "DB_USR" in os.environ else None,
        "db_name": os.environ["DB_NAME"],
        "db_host": os.environ["DB_HOST"] if "DB_HOST" in os.environ else None,
        "schema_name": "noaa_surveys"
    }
