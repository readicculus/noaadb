import json
import os

import cv2
import numpy as np
import sklearn.metrics as metrics

from noaadb import Session
from noaadb.schema.models import *

conf_thresh = .25
NAMES = {0: "Ringed Seal", 1: "Bearded Seal"}
FN_PATH = '/data2/luigi/evaluate/eo416x416_yolov3_tiny_3l'
os.makedirs(FN_PATH, exist_ok=True)
# darknet_image_list = '/fast/generated_data/IR/2020-12-25/valid/images.txt'
# darknet_image_list = '/fast/generated_data/IR/2020-12-25/test/images.txt'
darknet_image_list = '/fast/generated_data/EO/seals_416x416_v3/test/images.txt'
# dets_file = '/home/yuval/Documents/XNOR/darknet/results/detections/detections.json'
dets_file = '/home/yuval/Documents/XNOR/darknet/results/detections/detections_test.json'
# dets_file = '/home/yuval/Documents/XNOR/darknet/results/detections/detections_seals_416x416eo_test_yolov3-tiny-3l.json'

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
    label_files = []
    gt_dict = {}
    with open(darknet_image_list, 'r') as f:
        for l in f.readlines():
            img_fn = l.strip()
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
    fn_ct = {}
    for k in fns:
        for fn in fns[k]:
            class_id = fn['class_id']
            if not class_id in fn_ct:
                fn_ct[class_id] = 0
            fn_ct[class_id] += 1

    fp_ct = {}
    for k in fps:
        for l in fps[k]:
            class_id = l['class_id']
            if not class_id in fp_ct:
                fp_ct[class_id] = 0
            fp_ct[class_id] += 1

    fp_dup_ct = {}
    for k in duplicates:
        for l in duplicates[k]:
            class_id = l['class_id']
            if not class_id in fp_dup_ct:
                fp_dup_ct[class_id] = 0
            fp_dup_ct[class_id] += 1

    tp_ct = {}
    for k in tps:
        lbls = tps[k]
        for l in lbls.values():
            if l['det'] is not None:
                class_id = l['det']['class_id']
                if not class_id in tp_ct:
                    tp_ct[class_id] = 0
                tp_ct[class_id] += 1

    return tp_ct, fn_ct, fp_ct, fp_dup_ct

def draw_yolo2rect_box(l, im_color, color):
    h, w,_ = im_color.shape
    lw = l['w'] * w
    lh = l['h'] * h
    lx = l['x'] * w
    ly = l['y'] * h
    x1 = int(lx - (lw / 2))
    x2 = int(lx + (lw / 2))
    y1 = int(ly - (lh / 2))
    y2 = int(ly + (lh / 2))
    cv2.rectangle(im_color, (x1, y1), (x2, y2), color, 1)

def plot_false_negatives(fns, tps, fps):
    s=Session()
    for k in fns:
        lst = fns[k]
        if len(lst) == 0:
            continue
        tps_for_im = tps[k]
        fps_for_im = fps[k]
        db_im = s.query(IRImage).filter(IRImage.filename.ilike('%' + k[:-3] + '%')).first()
        im = db_im.ocv_load_normed()
        im_color = cv2.cvtColor(im, cv2.COLOR_GRAY2RGB)
        fp_color = (147,20,255)
        tp_color = (0, 255, 0)
        fn_color = (0, 0, 255)
        for l in lst:
            draw_yolo2rect_box(l, im_color, fn_color)
        for l in fps_for_im:
            draw_yolo2rect_box(l, im_color, fp_color)
        for _, v in tps_for_im.items():
            if v['det'] is not None:
                draw_yolo2rect_box(v['det'], im_color, tp_color)
        fp_out = os.path.join(FN_PATH, k)
        fontScale = .5
        font = cv2.FONT_HERSHEY_SIMPLEX
        lineType = 1
        cv2.putText(im_color,  "True Positive", (0,15),font,fontScale,tp_color,lineType)
        cv2.putText(im_color, "False Positive",(0,25),  font,fontScale,fp_color,lineType)
        cv2.putText(im_color,  "False Negative", (0,35),font,fontScale,fn_color,lineType)
        cv2.imwrite(fp_out, im_color)
    s.close()
    x=1

def ROC(gts, dets, points = 31):
    confidences = np.linspace(0,1,points)
    tpr_by_class = {0: np.zeros(points), 1: np.zeros(points)}
    fpr_by_class = {0: np.zeros(points), 1: np.zeros(points)}
    fn_by_class = {0: np.zeros(points), 1: np.zeros(points)}

    total_gt = {}
    for i, c in enumerate(confidences):
        tps, fns, fps, duplicates, thresh_filtered_ct = merge(gts, dets, c)
        tp_ct, fn_ct, fp_ct, fp_dup_ct = counts(tps, fns, fps, duplicates)
        for class_id in tp_ct:
            tpr_by_class[class_id][i] = tp_ct[class_id]
        for class_id in fn_ct:
            fn_by_class[class_id][i] = fn_ct[class_id]
        for class_id in fp_ct:
            dup = duplicates.get(class_id)
            fpr_by_class[class_id][i] = fp_ct[class_id] + (0 if dup is None else dup)


        # fpr[i] = fp_ct + fp_dup_ct
        for k in tp_ct:
            total_gt[k] = fn_ct[k] + tp_ct[k]
            # total_gt = fn_ct + tp_ct
        # assert(total_gt == fn_ct + tp_ct)
    auc_by_class = {}

    for k in [0, 1]:
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
                 label='%s, AUC = %0.2f' % (NAMES[k], auc_by_class[k]))

    plotted_confs = False
    for k in tpr_by_class:
        for i, xy in enumerate(zip(fpr_by_class[k], tpr_by_class[k])):  # <--
            x, y = xy
            if i % 4 == 0:
                if not plotted_confs:
                    ax.annotate('%.2f' % confidences[i], xy=(x + .02, y - .02), textcoords='data')
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

ROC(gts,dets)
tps, fns, fps, duplicates, thresh_filtered_ct = merge(gts, dets, conf_thresh)
tp_ct, fn_ct, fp_ct, fp_dup_ct = counts(tps, fns, fps, duplicates)

print("Results @ %.2f confidence threshold -- Total truths: %d" % (conf_thresh, tp_ct + fn_ct))
print('TP: %d, FN: %d, FP: %d (fp: %d, duplicates: %d) -- filtered %d boxes'
      % (tp_ct, fn_ct, fp_ct+fp_dup_ct, fp_ct, fp_dup_ct, thresh_filtered_ct))
# plot_false_negatives(fns, tps, fps)