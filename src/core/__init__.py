import logging
import time

import luigi
from dotenv import find_dotenv, load_dotenv

from core.task import ForcibleTask, AlwaysRunTask
from core.target import SQLAlchemyCustomTarget
import os
current_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_dir)

# Load environment
luigi_env = os.path.join(parent_dir, 'luigi.env')
load_dotenv(find_dotenv(filename=luigi_env))

# load project config
luigi_project_config = os.path.join(current_dir, 'luigi.cfg')
luigi.configuration.add_config_path(luigi_project_config)

log_dir = os.environ.get('LOG_OUTPUT_ROOT')
if log_dir is None: log_dir = os.path.join(parent_dir, 'log')
timestr = time.strftime("%y%m%d_%H%M%s")
log_fp = os.path.join(log_dir, '%s_core_debug.log' % timestr)
os.makedirs(log_dir, exist_ok=True)
logger = logging.getLogger('core_debug')
logger.setLevel(logging.DEBUG)
logger.propagate = False
fh = logging.FileHandler(log_fp, mode='w')
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

__all__ = ["ForcibleTask", "SQLAlchemyCustomTarget", "AlwaysRunTask"]