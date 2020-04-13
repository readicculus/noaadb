import json
import hashlib
import time

from flask import request

from noaadb.api.config import db
from noaadb.api.models import label_schema, images_schema, labels_schema, jobs_schema, workers_schema, species_schema
from noaadb.schema.models import NOAAImage, Job, Worker, Species

default_filter_options = \
    {
      "species_list": [
        "Ringed Seal",
        "Bearded Seal"
        "Polar Bear"
        "UNK Seal"
      ],
      "image_type": "eo",
      "show_shadows": False,
      "worker": "any",
      "job": "any",
      "invalid_labels_only": False,
      "exclude_invalid_labels": True,
      "show_removed": False
    }


def bool_type(str):
    s=str.lower()
    if s == "true":
        return True
    elif s == "false":
        return False
    raise Exception("Invalid bool type %s" % str)

def validate_filter_opts(req):
    filter_params = {}
    for k in default_filter_options:
        if not k in req:
            filter_params[k] = default_filter_options[k]
        else:
            if k in ["invalid_labels_only","show_shadows", "show_removed","exclude_invalid_labels"]:
                filter_params[k] = bool_type(req[k])
            else:
                filter_params[k] = req[k]
    return filter_params

def get_all_images(unique_image_ids):
    images = db.session.query(NOAAImage).options(db.defer('meta')).filter(NOAAImage.id.in_(unique_image_ids)).all()
    return images

def is_valid_label(im, label):
    if label['x1'] is None or label['y1'] is None or label['x2'] is None or label['y2'] is None:
        return False
    if label['x1'] < 0 or label['y1'] < 0:
        return False
    if label['x2'] > im['width'] or label['y2'] > im['height']:
        return False
    if label['end_date'] != None:
        return False
    return True

def combind_images_labels_to_json(images, labels):
    res = {"images":{}, "labels":{}, "invalid": {}}
    start = time.time()
    serialized_ims = images_schema.dump(images)
    print("serialized_ims time: %.4f" % (time.time() - start))
    start = time.time()
    serialized_labels = labels_schema.dump(labels)
    print("serialized_labels time: %.4f" % (time.time() - start))

    for image in serialized_ims:
        res["images"][image['id']] = image
    for label in serialized_labels:
        label_im_id = label['image_id']
        if not label_im_id in res["labels"]:
            res["labels"][label_im_id] = []
        res["labels"][label_im_id].append(label)

        valid = is_valid_label(res['images'][label_im_id], label)
        if not valid:
            if not label_im_id in res["invalid"]:
                res["invalid"][label_im_id] = []
            res["invalid"][label_im_id].append({"label_id":label["id"]})

    return res

def make_cache_key(*args, **kwargs):
    payload = request.json
    opts = validate_filter_opts(payload)
    res = json.dumps(opts, sort_keys=True, separators=(',', ':'))
    hash_object = hashlib.sha1(res.encode('utf-8'))
    return hash_object.hexdigest()

def get_species_dict():
    species=db.session.query(Species).all()
    species_dump=species_schema.dump(species)
    res = {}
    for item in species_dump:
        res[item["id"]] = item["name"]
    return res

def get_workers_dict():
    workers = db.session.query(Worker).all()
    workers_dump = workers_schema.dump(workers)
    res = {}
    for item in workers_dump:
        res[item["id"]] = {"name": item["name"], "human": item["human"]}
    return res

def get_jobs_dict():
    jobs=db.session.query(Job).all()
    jobs_dump=jobs_schema.dump(jobs)
    res = {}
    for item in jobs_dump:
        res[item["id"]] = {"job_name":item["job_name"], "notes":item["notes"]}
    return res