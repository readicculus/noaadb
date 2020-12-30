'''
Created on Feb 20, 2017
@author: jumabek
'''
from os import listdir
from os.path import isfile, join
import argparse
# import cv2
import numpy as np
import sys
import os
import shutil
import random
import math
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns; sns.set()  # for plot styling


def gen_anchors(lines, output_dir, dataset_dimensions = (416, 416)):
    (d_w, d_h) = dataset_dimensions

    annotation_dims = []

    size = np.zeros((1, 1, 3))
    for line in lines:

        print(line)
        f2 = open(line)
        for line in f2.readlines():
            line = line.rstrip('\n')
            w, h = line.split(' ')[3:]
            # print(w,h)
            annotation_dims.append([float(w), float(h)])
    annotation_dims = np.array(annotation_dims)
    annotation_dims[:,0] *= d_w
    annotation_dims[:,1] *= d_h
    from sklearn.cluster import KMeans
    kmeans3 = KMeans(n_clusters=9, init='random',n_init=1)
    kmeans3.fit(annotation_dims)
    y_kmeans3 = kmeans3.predict(annotation_dims)
    centers3 = kmeans3.cluster_centers_

    # yolo_anchor_average = []
    # for ind in range(9):
    #     yolo_anchor_average.append(np.mean(annotation_dims[y_kmeans3 == ind], axis=0))
    #
    # yolo_anchor_average = np.array(yolo_anchor_average)

    plt.scatter(annotation_dims[:, 0], annotation_dims[:, 1], c=y_kmeans3, s=2, cmap='viridis')
    plt.scatter(centers3[:, 0], centers3[:, 1], c='red', s=50)
    plt.savefig(os.path.join(output_dir, 'anchors_scatter.jpg'))

    fig, ax = plt.subplots()
    for ind in range(9):
        rectangle = plt.Rectangle((304 - centers3[ind, 0] / 2, 304 - centers3[ind, 1] / 2),
                                  centers3[ind, 0], centers3[ind, 1], fc='b', edgecolor='b', fill=None)
        ax.add_patch(rectangle)
    ax.set_aspect(1.0)
    plt.axis([0, d_w, 0, d_h])
    plt.savefig(os.path.join(output_dir, 'anchors_boxes.jpg'))
    centers3.sort(axis=0)
    print("Your custom anchor boxes are {}".format(centers3))

    F = open("YOLOV3_BDD_Anchors.txt", "w")
    F.write("{}".format(yoloV3anchors))
    F.close()
