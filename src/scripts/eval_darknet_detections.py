import json
import os

import cv2
import numpy as np
import sklearn.metrics as metrics

from noaadb import Session
from noaadb.schema.models import *

# darknet_image_list = '/fast/generated_data/IR/2020-12-25/valid/images.txt'
# darknet_image_list = '/fast/generated_data/IR/2020-12-25/test/images.txt'
# darknet_image_list = '/fast/generated_data/EO/seals_416x416_v3/test/images.txt'
# dets_file = '/home/yuval/Documents/XNOR/darknet/results/detections/detections.json'
# dets_file = '/home/yuval/Documents/XNOR/darknet/results/detections/detections_test.json'

#
#EO 512x512
conf_thresh = .25
POINTS=31
IMAGE_TYPE='EO'
NAMES = {0: "Ringed Seal", 1: "Bearded Seal"}
FN_PATH = '/fast/generated_data/EO/seals_512x512_noUNK/models/yolov3-tiny_3l/eval_examples'
darknet_image_list = '/fast/generated_data/EO/seals_512x512_noUNK/test_merged.txt'
dets_file = '/fast/generated_data/EO/seals_512x512_noUNK/models/yolov3-tiny_3l/detections_test.json'

#EO 416x416
# POINTS=31
# conf_thresh = .25
# POINTS=31
# IMAGE_TYPE='EO'
# NAMES = {0: "Ringed Seal", 1: "Bearded Seal"}
# FN_PATH = '/fast/generated_data/EO/seals_416x416_v2/models/yolov3-tiny_3l/eval_examples'
# darknet_image_list = '/fast/generated_data/EO/seals_416x416_v3/test/images.txt'
# dets_file = '/fast/generated_data/EO/seals_416x416_v2/models/yolov3-tiny_3l/detections_test.json'

# EO Polarbear
# conf_thresh = .1
# POINTS=31
# IMAGE_TYPE='EO'
# NAMES = {0: "Polar Bear"}
# FN_PATH = '/fast/generated_data/EO/polarbear_416x416_v1/models/yolov3_tiny_3l/eval_examples'
# darknet_image_list = '/fast/generated_data/EO/polarbear_416x416_v1/test/images.txt'
# dets_file = '/fast/generated_data/EO/polarbear_416x416_v1/models/yolov3_tiny_3l/darknet_detections.json'

# IR Yolo 80 1L
# IMAGE_TYPE='IR'
# POINTS=31
# NAMES = {0: "Hotspot"}
# FN_PATH = '/fast/generated_data/IR/2021-1-1/eval/yolo_feature_1l_v2/FN_examples/'
# darknet_image_list = '/fast/generated_data/IR/2021-1-1/test/images.txt'
# dets_file = '/home/yuval/Documents/XNOR/darknet/results/detections/ir_yolo_80_1l_detections.json'

os.makedirs(FN_PATH, exist_ok=True)
label_files = []
image_files = []

# == Load Detections ==
# loads the detections from darknet detections.json output(custom code required in darknet for this)
def load_detections(detections_json):
    with open(detections_json, 'r') as f:
        res = json.loads(f.read())

    im_to_dets = {}

    for im in res:
        bn = os.path.basename(im['image'])
        if bn not in im_to_dets:
            im_to_dets[bn] = []
        im_to_dets[bn].append(im)
    return im_to_dets

# == Load Ground Truths ==
# loads the ground truth labels given a darknet image list
def load_gts(darknet_image_list):
    gt_dict = {}
    with open(darknet_image_list, 'r') as f:
        for l in f.readlines():
            img_fn = l.strip()
            image_files.append(img_fn)
            label_fn = ".".join(l.split('.')[:-1]) + ".txt"
            label_files.append((img_fn, label_fn))

    for (img_fn, label_fn) in label_files:
        img_fn = os.path.basename(img_fn)
        if img_fn not in gt_dict:
            gt_dict[img_fn] = []
        with open(label_fn, 'r') as f:
            idx = 0
            for line in f.readlines():
                class_id, cx, cy, w, h = line.strip().split(" ")
                class_id, cx, cy, w, h = int(class_id), float(cx), float(cy), float(w), float(h)
                gt={'class_id': class_id, 'x':cx, 'y':cy, 'w':w, 'h':h, 'index': idx}
                gt_dict[img_fn].append(gt)
                idx += 1
    return gt_dict

# == Merge ground truths and detections ==
def merge(ground_truths, detections, thresh = .2):
    tps = {}
    fns = {} # {im_fn : list of ground truths that were missed}
    fps = {} # {im_fn : list of false positive detections}
    duplicates = {}  # {im_fn : list of false positive detections/duplicates}
    num_filtered = 0
    for im_k in ground_truths:
        fns[im_k] = []
        fps[im_k] = []
        duplicates[im_k] = []
        tps[im_k] = {}
        if im_k not in detections:
            fns[im_k] = ground_truths[im_k]

    for im_k in detections:
        k_dets = detections[im_k]
        k_gts = ground_truths.get(im_k)
        det_dict = {}
        for det_i, gt in enumerate(k_gts):
            det_dict[det_i] = {'gt': gt, 'det': None}

        for det in k_dets:
            if det['prob'] < thresh:
                num_filtered+=1
                continue

            if det['tp']:
                if  det_dict[det['truth_index_in_file']]['det'] == None:
                    det_dict[det['truth_index_in_file']]['det'] = det
                else:
                    det['duplicate'] = 1
                    duplicates[im_k].append(det)
            elif det['duplicate']:
                duplicates[im_k].append(det)
            elif det['fp']:
                det['duplicate'] = 0
                fps[im_k].append(det)

        tps[im_k] = det_dict
        for k in det_dict:
            if det_dict[k]['det'] == None:
                fns[im_k].append(det_dict[k]['gt'])

    return tps, fns, fps, duplicates, num_filtered

# === Count the TP, FP, FN, FP_dup dictionaries ==  #
def counts(tps, fns, fps, duplicates):
    fn_ct = {k:0 for k in list(NAMES.keys())}
    for k in fns:
        for fn in fns[k]:
            fn_ct[fn['class_id']] += 1

    fp_ct = {k:0 for k in list(NAMES.keys())}
    for k in fps:
        for l in fps[k]:
            fp_ct[l['class_id']] += 1

    fp_dup_ct = {k:0 for k in list(NAMES.keys())}
    for k in duplicates:
        for l in duplicates[k]:
            fp_dup_ct[l['class_id']] += 1

    tp_ct = {k:0 for k in list(NAMES.keys())}
    for k in tps:
        lbls = tps[k]
        for l in lbls.values():
            if l['det'] is not None:
                tp_ct[l['det']['class_id']] += 1

    return tp_ct, fn_ct, fp_ct, fp_dup_ct

def draw_yolo2rect_box(l, im_color, color, h, w, dx=0, dy=0, line_w=1):
    lw = l['w'] * w
    lh = l['h'] * h
    lx = l['x'] * w
    ly = l['y'] * h
    x1 = int(lx - (lw / 2)) + dx
    x2 = int(lx + (lw / 2)) + dx
    y1 = int(ly - (lh / 2)) + dy
    y2 = int(ly + (lh / 2)) + dy
    cv2.rectangle(im_color, (x1, y1), (x2, y2), color, line_w)
    fontScale = .5
    lineType = 1
    font = cv2.FONT_HERSHEY_SIMPLEX
    cv2.putText(im_color, NAMES[l['class_id']], (x1, y1), font, fontScale, color, lineType)


def plot_false_negative_chips(fns, tps, fps):
    keys = list(fns.keys())
    eo_partial_complete = {}
    for k in keys:
        if len(tps[k]) > 10:
            continue
        image_fp = None
        for fp in image_files:
            if k in fp:
                image_fp = fp
                break
        im_color = cv2.imread(image_fp)
        lw = 1
        h,w,_ = im_color.shape
        fontScale = .5
        lineType = 1
        font = cv2.FONT_HERSHEY_SIMPLEX

        fp_color = (147, 20, 255)
        tp_color = (0, 255, 0)
        fn_color = (0, 0, 255)

        for l in fns[k]:
            draw_yolo2rect_box(l, im_color, fn_color, h, w, line_w=lw)
        for l in fps[k]:
            draw_yolo2rect_box(l, im_color, fp_color, h, w, line_w=lw)

        for _, v in tps[k].items():
            if v['det'] is not None:
                draw_yolo2rect_box(v['det'], im_color, tp_color, h, w, line_w=lw)

        cv2.putText(im_color, "True Positive", (0, int(30 * fontScale)), font, fontScale, tp_color, lineType)
        cv2.putText(im_color, "False Positive", (0, int(50 * fontScale)), font, fontScale, fp_color, lineType)
        cv2.putText(im_color, "False Negative", (0, int(70 * fontScale)), font, fontScale, fn_color, lineType)
        fp_out = os.path.join(FN_PATH, k)
        cv2.imwrite(fp_out, im_color)

def plot_false_negatives(fns, tps, fps):
    s=Session()
    # keys = list(fns.keys()) + list(fps.keys())
    keys = list(fns.keys())
    eo_partial_complete = {}
    for k in keys:
        # if len(lst) == 0:
        #     continue

        # fps_for_im =
        if IMAGE_TYPE == 'IR':
            db_im = s.query(IRImage).filter(IRImage.filename.ilike('%' + k[:-3] + '%')).first()
            im = db_im.ocv_load_normed()
            im_color = cv2.cvtColor(im, cv2.COLOR_GRAY2RGB)
            h, w = 512,640
            fontScale = .5
            lineType = 1
            dx,dy = 0,0
            lw = 1
            fp_out = os.path.join(FN_PATH, k)
        else:
            fn = '%' + '_'.join(k.split('_')[:-4])+ '%'
            crops = k.replace('.jpg', '').split('_')[-4:]
            crops = [int(x) for x in crops]
            h = crops[3] - crops[1]
            w = crops[2] - crops[0]
            db_im = s.query(EOImage).filter(EOImage.filename.ilike(fn)).first()
            if len(fps[k]) == 0 and db_im.filename not in eo_partial_complete:
                continue
            if db_im.filename in eo_partial_complete:
                im_color = cv2.imread(eo_partial_complete[db_im.filename])
                fp_out = eo_partial_complete[db_im.filename]
            else:
                fp_out = os.path.join(FN_PATH, db_im.filename)
                eo_partial_complete[db_im.filename] = fp_out
                im_color = db_im.ocv_load()
            dx = crops[0]
            dy = crops[1]
            lw = 4
            fontScale = 2
            lineType = 2

        fp_color = (147,20,255)
        tp_color = (0, 255, 0)
        fn_color = (0, 0, 255)
        for l in fns[k]:
            draw_yolo2rect_box(l, im_color, fn_color, h,w,dx=dx, dy=dy, line_w=lw)
        for l in fps[k]:
            draw_yolo2rect_box(l, im_color, fp_color, h,w,dx=dx, dy=dy, line_w=lw)
        for _, v in tps[k].items():
            if v['det'] is not None:
                draw_yolo2rect_box(v['det'], im_color, tp_color, h,w,dx=dx, dy=dy, line_w=lw)

        font = cv2.FONT_HERSHEY_SIMPLEX
        cv2.putText(im_color,  "True Positive", (0,int(30*fontScale)),font,fontScale,tp_color,lineType)
        cv2.putText(im_color, "False Positive",(0,int(50*fontScale)),  font,fontScale,fp_color,lineType)
        cv2.putText(im_color,  "False Negative", (0,int(70*fontScale)),font,fontScale,fn_color,lineType)
        cv2.imwrite(fp_out, im_color)
    s.close()

def precision_recall(gts, dets, points = 31):
    confidences = np.linspace(0,1,points)
    precision_by_class = {k: np.zeros(points) for k in list(NAMES.keys())}
    recall_by_class = {k: np.zeros(points) for k in list(NAMES.keys())}

    for i, c in enumerate(confidences):
        tps, fns, fps, duplicates, thresh_filtered_ct = merge(gts, dets, c)
        tp_ct, fn_ct, fp_ct, fp_dup_ct = counts(tps, fns, fps, duplicates)
        for class_id in NAMES:
            dup = duplicates.get(class_id)
            fp = fp_ct[class_id] + (0 if dup is None else dup)
            precision_by_class[class_id][i] = 0 if (tp_ct[class_id] + fp) == 0 else tp_ct[class_id] / (tp_ct[class_id] + fp)
            recall_by_class[class_id][i] = tp_ct[class_id] / (tp_ct[class_id] + fn_ct[class_id])
    min_x = 1
    min_y = 1
    for class_id in NAMES:
        idxs = np.argwhere(precision_by_class[class_id] == 0)
        print("Precision points removed because 0:" + str(idxs))
        precision_by_class[class_id]=np.delete(precision_by_class[class_id], idxs)
        recall_by_class[class_id]=np.delete(recall_by_class[class_id], idxs)
        min_x = recall_by_class[class_id].min() if recall_by_class[class_id].min() < min_x else min_x
        min_y = precision_by_class[class_id].min() if precision_by_class[class_id].min() < min_y else min_y


    import matplotlib.pyplot as plt
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.margins(0.05)
    plt.title('Precision Recall Curve')
    colors = {0:'b',1:'g'}
    for k in list(NAMES.keys()):
        # plt.plot(recall_by_class[k], precision_by_class[k], colors[k],
        #          label='%s, AUC = %0.2f' % (NAMES[k], auc_by_class[k]))
        plt.plot(recall_by_class[k], precision_by_class[k], colors[k],
                 label='%s' % (NAMES[k]))

    plotted_confs = False
    m = int(POINTS/6)
    for k in list(NAMES.keys()):
        for i, xy in enumerate(zip(recall_by_class[k], precision_by_class[k])):  # <--
            x, y = xy
            if i % m == 0:
                if not plotted_confs:
                    ax.annotate('%.2f' % confidences[i], xy=(x + .02, y + .02), textcoords='data')
                ax.plot([x], [y], '.', color=colors[k])
        plotted_confs = True

    plt.legend(loc='lower right')
    # plt.plot([0, 1], [0, 1], 'r--')
    # ax.margins(0.1)
    plt.xlim([min_x, None])
    plt.ylim([min_y, None])
    plt.ylabel('Precision')
    plt.xlabel('Recall')
    plt.show()

def ROC(gts, dets, points = 31):
    confidences = np.linspace(0,1,points)
    tpr_by_class = {k: np.zeros(points) for k in list(NAMES.keys())}
    fpr_by_class = {k: np.zeros(points) for k in list(NAMES.keys())}
    fn_by_class = {k: np.zeros(points) for k in list(NAMES.keys())}

    total_gt = {}
    max_fp_by_class = {k: 0 for k in list(NAMES.keys())}
    for i, c in enumerate(confidences):
        tps, fns, fps, duplicates, thresh_filtered_ct = merge(gts, dets, c)
        tp_ct, fn_ct, fp_ct, fp_dup_ct = counts(tps, fns, fps, duplicates)
        for class_id in fp_ct:
            if fp_ct[class_id] > max_fp_by_class[class_id]:
                max_fp_by_class[class_id] = fp_ct[class_id]
        for class_id in tp_ct:
            tpr_by_class[class_id][i] = tp_ct[class_id]
        for class_id in fn_ct:
            fn_by_class[class_id][i] = fn_ct[class_id]
        for class_id in fp_ct:
            dup = duplicates.get(class_id)
            fpr_by_class[class_id][i] = fp_ct[class_id] + (0 if dup is None else dup)


        for k in tp_ct:
            total_gt[k] = fn_ct[k] + tp_ct[k]
    auc_by_class = {}

    for k in list(NAMES.keys()):
        tpr_by_class[k] = tpr_by_class[k]/total_gt[k]
        fpr_by_class[k] = fpr_by_class[k]/fpr_by_class[k].max()
        auc_by_class[k] = metrics.auc(fpr_by_class[k], tpr_by_class[k])
    import matplotlib.pyplot as plt
    fig = plt.figure()
    ax = fig.add_subplot(111)
    plt.title('Receiver Operating Characteristic')
    colors = {0:'b',1:'g'}
    for k in auc_by_class:
        plt.plot(fpr_by_class[k], tpr_by_class[k], colors[k],
                 label='%s\nAUC = %0.2f, GT = %d, maxFP = %d' %
                       (NAMES[k], auc_by_class[k], total_gt[k], max_fp_by_class[k]))

    plotted_confs = False
    m = int(POINTS/6)
    for k in tpr_by_class:
        for i, xy in enumerate(zip(fpr_by_class[k], tpr_by_class[k])):  # <--
            x, y = xy
            if i % m == 0:
                if not plotted_confs:
                    ax.annotate('%.2f' % confidences[i], xy=(x + .02, y - .03), textcoords='data')
                ax.plot([x], [y], '.', color=colors[k])
        plotted_confs = True

    plt.legend(loc='lower right')
    plt.plot([0, 1], [0, 1], 'r--')
    plt.xlim([0, 1])
    plt.ylim([0, 1])
    plt.ylabel('True Positive Rate')
    plt.xlabel('False Positive Rate')
    plt.show()

gts = load_gts(darknet_image_list)
dets = load_detections(dets_file)

precision_recall(gts, dets, points=POINTS)
ROC(gts, dets, points=POINTS)
tps, fns, fps, duplicates, thresh_filtered_ct = merge(gts, dets, conf_thresh)
tp_ct, fn_ct, fp_ct, fp_dup_ct = counts(tps, fns, fps, duplicates)

for class_id in tp_ct:
    print("Results @ %.2f confidence threshold -- Total truths: %d" % (conf_thresh, tp_ct[class_id] + fn_ct[class_id]))
    dups = fp_dup_ct.get(class_id) or 0
    print('TP: %d, FN: %d, FP: %d (fp: %d, duplicates: %d) -- filtered %d boxes'
          % (tp_ct[class_id], fn_ct[class_id], fp_ct[class_id]+dups, fp_ct[class_id], dups, thresh_filtered_ct))
# plot_false_negative_chips(fns, tps, fps)
# plot_false_negatives(fns, tps, fps)