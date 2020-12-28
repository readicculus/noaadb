import glob
import random
import shutil

import cv2
import luigi
import os

from core import ForcibleTask
from noaadb.schema.models import TrainTestValidEnum
from pipelines.yolo_dataset_export import datasetConfig, processingConfig


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
        output = self.output()
        self.output_targets = {'Train': {'type': TrainTestValidEnum.train,
                                         'dir': output['train_dir'],
                                         'image_list': output['train_list']},
                               'Test': {'type': TrainTestValidEnum.test,
                                        'dir': output['test_dir'],
                                        'image_list': output['test_list']},
                               'Valid': {'type': TrainTestValidEnum.valid,
                                         'dir': output['valid_dir'],
                                         'image_list': output['valid_list']},
                               }

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
                'examples_dir': luigi.LocalTarget(examples_dir)}

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

    def _get_name_id(self, name):
        mapped_names = []
        species_map = self.processing_cfg.species_map
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
        im = cv2.imread(image_path)
        labels_path = '.'.join(image_path.split('.')[:-1]) + '.txt'
        im_w = im.shape[1]
        im_h = im.shape[0]
        color_im = cv2.cvtColor(im, cv2.COLOR_GRAY2RGB)
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
