import os
import luigi
from dotenv import load_dotenv, find_dotenv

current_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_dir)

luigi_env = os.path.join(parent_dir, 'luigi.env')

load_dotenv(find_dotenv(filename=luigi_env))


class pipelineConfig(luigi.Config):
    output_root = luigi.Parameter()
    task_type = luigi.Parameter()
