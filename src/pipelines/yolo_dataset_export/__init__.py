import os
import luigi
from dotenv import load_dotenv, find_dotenv

pipeline_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(pipeline_dir)

luigi_env = os.path.join(parent_dir, 'luigi.env')

load_dotenv(find_dotenv(filename=luigi_env))

class datasetConfig(luigi.Config):
    max_objects_per_image = luigi.IntParameter(default=-1)
    combine_test_valid = luigi.BoolParameter(default=False)

class processingConfig(luigi.Config):
    fix_image_dimension = luigi.DictParameter(default=None)
    bbox_padding = luigi.IntParameter(default=0)
    species_map = luigi.DictParameter(default={})

class chipConfig(luigi.Config):
    chip_dim = luigi.IntParameter()  # chip dimension
    chip_stride = luigi.IntParameter()  # stride of chip
    label_overlap_threshold = luigi.FloatParameter(default=.5)

    def get_dir_id(self):
        return "%d_%d_%.2f" % (self.chip_dim, self.chip_stride, self.label_overlap_threshold)