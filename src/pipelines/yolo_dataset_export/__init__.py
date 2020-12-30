import os
import luigi
from dotenv import load_dotenv, find_dotenv

from noaadb.schema.models import TrainTestValidEnum

pipeline_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(pipeline_dir)

luigi_env = os.path.join(parent_dir, 'luigi.env')

load_dotenv(find_dotenv(filename=luigi_env))


# Ability to configure Train, Test, and Valid sets indavidually
class setConfig(luigi.Config):
    # max_objects_per_image = luigi.IntParameter(default=-1)
    only_manual_reviewed = luigi.BoolParameter(default=False)
    background_ratio = luigi.FloatParameter(default=0)  # 0 = no background, 1 = same # background as positive
    bbox_padding = luigi.IntParameter(default=0)


class testSetConfig(setConfig): pass
class trainSetConfig(setConfig): pass
class validSetConfig(setConfig): pass


class datasetConfig(object):
    def __init__(self):
        self.test_cfg = testSetConfig()
        self.train_cfg = trainSetConfig()
        self.valid_cfg = validSetConfig()

    def get_cfg(self, set_type: TrainTestValidEnum):
        if set_type == TrainTestValidEnum.train:
            return self.train_cfg
        elif set_type == TrainTestValidEnum.test:
            return self.test_cfg
        elif set_type == TrainTestValidEnum.valid:
            return self.valid_cfg
        raise Exception('Invalid set type: %s' % str(set_type))


class processingConfig(luigi.Config):
    fix_image_dimension = luigi.DictParameter(default=None)
    species_map = luigi.DictParameter(default={})


class chipConfig(luigi.Config):
    chip_h = luigi.IntParameter()  # chip dimension
    chip_w = luigi.IntParameter()  # chip dimension
    chip_stride_x = luigi.IntParameter()  # stride of chip
    chip_stride_y = luigi.IntParameter()  # stride of chip
    label_overlap_threshold = luigi.FloatParameter(default=.5)

    def get_dir_id(self):
        return "%dx%d_%dx%d_%.2f" % (self.chip_h, self.chip_w, self.chip_stride_x, self.chip_stride_y, self.label_overlap_threshold)
