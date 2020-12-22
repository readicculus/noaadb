import json
import os

import luigi

from pipelines.yolo_dataset_export import pipeline_dir
from pipelines.yolo_dataset_export.tasks import ExportYoloEODatasetTask
import numpy as np
from noaadb import Session
from noaadb.schema.models import *

def count_labels(image_list, task):
    label_files = []
    image_files = []
    with image_list.open('r') as f:
        for line in f.readlines():
            lbl_file = ".".join(line.strip("").split(".")[:-1])+".txt"
            label_files.append(lbl_file)
            image_files.append(line.strip())
    class_counts = {}
    widths = []
    heights = []
    for lbl_file, img_file in zip(label_files, image_files):
        # labels = []
        with open(lbl_file, 'r') as f:
            for line in f.readlines():
                class_id, cx, cy, w, h = line.strip().split(" ")
                class_id, cx, cy, w, h = int(class_id), float(cx), float(cy), float(w), float(h)
                if w > .5 or h > .5:
                    x= task.draw_labels(img_file)
                    a=1
                widths.append(w)
                heights.append(h)
                # labels.append((class_id, cx, cy, w, h))
                if class_id not in class_counts:
                    class_counts[class_id] = 0
                class_counts[class_id] += 1


    widths = np.array(widths)
    heights = np.array(heights)
    res = {'num_images': len(label_files), 'class_counts': class_counts}

if __name__ == '__main__':
    # s =  Session()
    # a = s.query(Annotation, BoundingBox).join(Annotation.eo_box).filter(BoundingBox.width > 500).all()
    # for el, box in a:
    #     box = el.eo_box
    #     cx = box.cx
    #     cy = box.cy
    #     box.x1 = cx
    #     box.x2 = cx
    #     box.y1=cy
    #     box.y2=cy
    #     s.flush()
    # s.commit()
    # s.close()
    luigi_project_config = os.path.join(pipeline_dir, 'export_eo_dataset.cfg')
    luigi.configuration.add_config_path(luigi_project_config)
    task = ExportYoloEODatasetTask()
    output = task.output()
    train_label_counts = count_labels(output['train_list'], task)

