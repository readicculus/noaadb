import os

import luigi
from dotenv import load_dotenv, find_dotenv

from noaadb.sqlite.image_registry import SqliteImageRegistry

current_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_dir)

luigi_env = os.path.join(parent_dir, 'luigi.env')
luigi_project_config = os.path.join(current_dir, 'luigi.cfg')

load_dotenv(find_dotenv(filename=luigi_env))
luigi.configuration.add_config_path(luigi_project_config)

class imageRegistryConfig(luigi.Config):
    sqlite_path = luigi.Parameter()

    def get_registry(self) -> SqliteImageRegistry:
        return SqliteImageRegistry(self.sqlite_path)

