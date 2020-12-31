import glob
import logging
import random
import shutil
import time

import cv2
import luigi
import os
import pandas as pd

from core import ForcibleTask
from noaadb.schema.models import TrainTestValidEnum
from pipelines.yolo_dataset_export import datasetConfig, processingConfig, DATASET_LOG_NAME
# from pipelines.yolo_dataset_export.tasks.gen_anchors import gen_anchors
from pipelines.yolo_dataset_export.tasks.plots import draw_label_plots

# ========= Helper functions =========
def read_image_list(target):
    lines = []
    with target.open('r') as f:
        for l in f.readlines():
            lines.append(l.strip())
    return lines


def get_label_files(image_list_target):
    images = read_image_list(image_list_target)
    label_files = []
    for im in images:
        lbl_fp = '.'.join(im.split('.')[:-1]) + '.txt'
        label_files.append(lbl_fp)
    return label_files

def load_labels(image_list_target, flatten=False):
    label_files = get_label_files(image_list_target)
    labels = {}
    if flatten: labels = []
    for fp in label_files:
        if not flatten: labels[fp] = []
        with open(fp, 'r') as f:
            for line in f.readlines():
                class_id, cx, cy, w, h = line.strip().split(" ")
                class_id, cx, cy, w, h = int(class_id), float(cx), float(cy), float(w), float(h)
                if flatten:
                    labels.append({'class_id': class_id, 'x': cx, 'y': cy, 'w': w, 'h': h})
                else:
                    labels[fp].append({'class_id': class_id, 'x': cx, 'y': cy, 'w': w, 'h': h})
    return labels

def load_names(target):
    names = []
    with target.open('r') as f:
        for l in f.readlines():
            names.append(l.strip())
    return names

# ========= Base Task =========
def post_run_wrapper(func):
    # Wrap the run task so we can generate dataset stats and validate dataset after they are created
    def wrapper(self, *args, **kwargs):
        ret = func(self, *args, **kwargs)
        self.generate_stats()
        self.validate_dataset()
        return ret

    return wrapper

def complete_wrapper(func):
    # Wrap the run task so we can generate dataset stats and validate dataset after they are created
    def wrapper(self, *args, **kwargs):
        is_complete = func(self, *args, **kwargs)
        if is_complete:
            self.generate_stats()
            self.validate_dataset()
        return is_complete

    return wrapper

class DarknetDatasetTask(ForcibleTask):
    dataset_root = luigi.Parameter()
    dataset_name = luigi.Parameter()
    num_examples = luigi.IntParameter(default=20)
    delete_images_on_rerun = luigi.BoolParameter()

    _names = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataset_cfg = datasetConfig()
        self.processing_cfg = processingConfig()
        os.makedirs(self.output()['train_dir'].path, exist_ok=True)
        os.makedirs(self.output()['test_dir'].path, exist_ok=True)
        os.makedirs(self.output()['valid_dir'].path, exist_ok=True)
        os.makedirs(self.output()['examples_dir'].path, exist_ok=True)
        os.makedirs(self.output()['backup_dir'].path, exist_ok=True)
        os.makedirs(self.output()['stats_dir'].path, exist_ok=True)

        # add dataset loggers
        timestr = time.strftime("%y%m%d_%H%M%s")
        log_fp = os.path.join(self.output()['dataset_dir'].path, 'log', '%s_create_dataset.log' % timestr)
        os.makedirs(os.path.join(self.output()['dataset_dir'].path, 'log'), exist_ok=True)
        self.dataset_log = logging.getLogger(DATASET_LOG_NAME)
        self.dataset_log.setLevel(logging.DEBUG)
        self.dataset_log.propagate = False
        fh = logging.FileHandler(log_fp, mode='w')
        fh.setLevel(logging.DEBUG)
        self.dataset_log.addHandler(fh)

        self.interface_log = logging.getLogger('luigi-interface')


    def __init_subclass__(cls):
        super().__init_subclass__()
        # override task run with our wrapper run function that also cleans up
        cls.run = post_run_wrapper(cls.run)
        cls.complete = complete_wrapper(cls.complete)

    def gen_anchors(self):
        output = self.output()
        l = get_label_files(output['train_list'])
        l+= get_label_files(output['test_list'])
        l+= get_label_files(output['valid_list'])
        # gen_anchors(l, output['stats_dir'].path)

    def generate_stats(self):
        # self.gen_anchors()
        self.interface_log.info('='*50)
        self.interface_log.info("Creating dataset stats/figures")
        output = self.output()
        names = load_names(output['yolo_names_file'])
        train_labels = load_labels(output['train_list'], flatten=True)
        test_labels = load_labels(output['test_list'], flatten=True)
        valid_labels = load_labels(output['valid_list'], flatten=True)
        stats_dir = output['stats_dir'].path

        for labels, pre in zip([train_labels, test_labels, valid_labels], ['train', 'test', 'valid']):
            x = pd.DataFrame(labels)
            draw_label_plots(x, stats_dir, names, fn_prefix=pre)
        self.interface_log.info("COMPLETE: Creating dataset stats/figures")
        self.interface_log.info('='*50)

    def validate_dataset(self):
        self.interface_log.info('='*50)
        self.interface_log.info("Validating Datase")
        # validate no labels outside of image
        output = self.output()
        names = load_names(output['yolo_names_file'])
        train_labels = load_labels(output['train_list'], flatten=True)
        test_labels = load_labels(output['test_list'], flatten=True)
        valid_labels = load_labels(output['valid_list'], flatten=True)
        all = train_labels+test_labels+valid_labels
        df = pd.DataFrame(all)
        x1 = df['x'] - (df['w'] / 2.)
        x2 = df['x'] + (df['w'] / 2.)
        y1 = df['y'] - (df['h'] / 2.)
        y2 = df['y'] + (df['h'] / 2.)
        PAD = 0.0001
        assert (x1 >= -PAD).all()
        assert (x2 <= 1+PAD).all()
        assert (x1 <= x2).all()
        assert (y1 >= -PAD).all()
        assert (y2 <= 1+PAD).all()
        assert (y1 <= y2).all()
        self.interface_log.info("Validation Complete!")
        self.interface_log.info('='*50)

    def get_output_by_enum(self, train_test_valid_enum):
        output = self.output()
        output_targets_by_enum = {
            TrainTestValidEnum.train: {'dir': output['train_dir'],
                                       'image_list': output['train_list']},
            TrainTestValidEnum.test: {'dir': output['test_dir'],
                                      'image_list': output['test_list']},
            TrainTestValidEnum.valid: {'dir': output['valid_dir'],
                                       'image_list': output['valid_list']},
        }
        return output_targets_by_enum[train_test_valid_enum]

    def get_outputs_by_name(self):
        output = self.output()
        return {'Train': {'type': TrainTestValidEnum.train,
                                        'dir': output['train_dir'],
                                        'image_list': output['train_list']},
                              'Test': {'type': TrainTestValidEnum.test,
                                       'dir': output['test_dir'],
                                       'image_list': output['test_list']},
                              'Valid': {'type': TrainTestValidEnum.valid,
                                        'dir': output['valid_dir'],
                                        'image_list': output['valid_list']},
                              }

    # def __init_subclass__(cls):
    #     super().__init_subclass__()
    #     cls.dataset_cfg = datasetConfig()
    #     cls.processing_cfg = processingConfig()

    def output(self):
        dataset_dir = os.path.join(str(self.dataset_root), str(self.dataset_name))
        train_dir = os.path.join(dataset_dir, 'train')
        train_list = os.path.join(train_dir, 'images.txt')
        test_dir = os.path.join(dataset_dir, 'test')
        test_list = os.path.join(test_dir, 'images.txt')
        valid_dir = os.path.join(dataset_dir, 'valid')
        valid_list = os.path.join(valid_dir, 'images.txt')
        yolo_data_file = os.path.join(dataset_dir, 'yolo.data')
        yolo_names_file = os.path.join(dataset_dir, 'yolo.names')
        backup_dir = os.path.join(dataset_dir, 'backup')
        examples_dir = os.path.join(dataset_dir, 'examples')
        stats_dir = os.path.join(dataset_dir, 'dataset_stats')
        complete_file = os.path.join(dataset_dir, 'complete.txt')

        return {'dataset_dir': luigi.LocalTarget(dataset_dir),
                'train_dir': luigi.LocalTarget(train_dir),
                'test_dir': luigi.LocalTarget(test_dir),
                'valid_dir': luigi.LocalTarget(valid_dir),
                'train_list': luigi.LocalTarget(train_list),
                'test_list': luigi.LocalTarget(test_list),
                'valid_list': luigi.LocalTarget(valid_list),
                'yolo_data_file': luigi.LocalTarget(yolo_data_file),
                'yolo_names_file': luigi.LocalTarget(yolo_names_file),
                'backup_dir': luigi.LocalTarget(backup_dir),
                'examples_dir': luigi.LocalTarget(examples_dir),
                'stats_dir': luigi.LocalTarget(stats_dir),
                'complete_file': luigi.LocalTarget(complete_file)}

    def complete(self):
        return self.output()['complete_file'].exists()

    def cleanup(self):
        print("This task was forced.")
        print("About to cleanup dataset: %s" % self.output()['dataset_dir'].path)
        print("If you are ok with continuing type YES.")
        response = input()
        if response != 'YES':
            raise Exception("Ran a forced task and did not confirm with 'YES'")

        loop = True
        while loop:
            print("Would you like to delete images in the train/test/valid directories?")
            print("[1] to KEEP the images(makes re-running faster as skipps processing images if already there)")
            print("[2] to DELETE the images and re-create from scratch")
            response = input()
            if response == '1':
                self._delete_non_image_files(self.output()['train_dir'].path)
                self._delete_non_image_files(self.output()['test_dir'].path)
                self._delete_non_image_files(self.output()['valid_dir'].path)
                loop = False
            elif response == '2':
                self._delete_dir(self.output()['examples_dir'].path)
                self._delete_dir(self.output()['train_dir'].path)
                self._delete_dir(self.output()['test_dir'].path)
                self._delete_dir(self.output()['valid_dir'].path)
                loop=False
            else:
                print("Invalid entry: %s.  Try again." % response)
            # also delete examples and yolo names/data files
            self._delete_dir(self.output()['examples_dir'].path)
            os.remove(self.output()['yolo_data_file'].path)
            os.remove(self.output()['yolo_names_file'].path)

    def _get_name_id(self, name,set_type):
        mapped_names = []
        cfg = self.dataset_cfg.get_cfg(set_type)
        species_map = cfg.species_map
        if "*" not in species_map and name not in species_map:
            mapped_names.append(name)
        else:
            for src, dsts in species_map.items():
                if src == "*" or src == name:
                    for dst in dsts:
                        mapped_names.append(dst)
                    break

        for mn in mapped_names:
            if mn not in self._names:
                id = len(self._names)
                self._names[mn] = id
        return [self._names[mn] for mn in mapped_names]

    def _save_names_file(self):
        names_in_order = [k for k, v in sorted(self._names.items(), key=lambda item: item[1])]
        with self.output()['yolo_names_file'].open('w') as f:
            for name in names_in_order:
                f.write('%s\n' % name)

    def _save_data_file(self):
        output = self.output()
        data_file = "classes = %d\n" \
                    "train = %s\n" \
                    "test = %s\n" \
                    "valid = %s\n" \
                    "names = %s\n" \
                    "backup = %s\n" % (len(self._names),
                                       output['train_list'].path,
                                       output['test_list'].path,
                                       output['valid_list'].path,
                                       output['yolo_names_file'].path,
                                       output['backup_dir'].path)
        with output['yolo_data_file'].open('w') as f:
            f.write(data_file)

    def _delete_dir(self, directory):
        if os.path.exists(directory):
            shutil.rmtree(directory)

    def _delete_non_image_files(self, directory):
        files = glob.glob(os.path.join(directory, '*.txt'))
        for file in files:
            os.remove(file)

    def _draw_labels(self, image_path):
        color_im = cv2.imread(image_path, cv2.IMREAD_UNCHANGED)
        if len(color_im.shape) == 2:
            # if ir convert to rgb
            color_im = cv2.cvtColor(color_im, cv2.COLOR_GRAY2RGB)

        labels_path = '.'.join(image_path.split('.')[:-1]) + '.txt'
        im_w = color_im.shape[1]
        im_h = color_im.shape[0]
        with open(labels_path, 'r') as f:
            # <object-class> <x_center> <y_center> <width> <height>
            for l in f.readlines():
                label = l.strip()
                obj_class, x_cent, y_cent, w, h = label.split(' ')
                obj_class, x_cent, y_cent, w, h = \
                    (int(obj_class), float(x_cent), float(y_cent), float(w), float(h))
                aw = w * im_w
                ah = h * im_h
                ax = x_cent * im_w
                ay = y_cent * im_h
                x1 = int(ax - (aw / 2))
                x2 = int(ax + (aw / 2))
                y1 = int(ay - (ah / 2))
                y2 = int(ay + (ah / 2))
                cv2.rectangle(color_im, (x1, y1), (x2, y2), (0, 255, 0), thickness=1)
        return color_im

    def generate_examples(self, list_name, num_to_draw):
        output = self.output()
        examples_dir = output['examples_dir'].path
        out_dir = os.path.join(examples_dir, list_name)
        if os.path.exists(out_dir):
            shutil.rmtree(out_dir)
        os.makedirs(out_dir)

        images = []
        with output[list_name].open('r') as f:
            for line in f.readlines():
                images.append(line.strip())
        if num_to_draw > len(images) or num_to_draw == -1:
            num_to_draw = len(images)
        selected = random.sample(images, num_to_draw)
        for image_fp in selected:
            out_path = os.path.join(out_dir, os.path.basename(image_fp))
            im = self._draw_labels(image_fp)
            cv2.imwrite(out_path, im)
