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
        "Bearded Seal",
        "Polar Bear",
        "UNK Seal"
      ],
      "image_type": "eo",
      "show_shadows": False,
      "workers": [],
      "jobs": [],
      "surveys": [],
      "flights": [],
      "camera_positions": [],
      "exclude_invalid_labels": True,
      "show_removed_labels": False
    }


def validate_filter_opts(req):
    filter_params = {}
    errors = {}
    for k in default_filter_options:
        if not k in req:
            filter_params[k] = default_filter_options[k]
        else:
            if k in ["show_shadows", "show_removed_labels","exclude_invalid_labels"]:
                if isinstance(req[k], bool):
                    filter_params[k] = req[k]
                else:
                    errors[k] = "Invalid boolean %s, use true or false (without quotes)" % req[k]
            elif k == "image_type":
                if not req[k] in ["eo", "ir"]:
                    errors[k] = "Invalid image_type %s, following are accepted options ['eo', 'ir']." % req[k]
                else:
                    filter_params[k] = req[k]

            else:
                filter_params[k] = req[k]
    return filter_params, errors

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
    res = {"images":{}, "labels":{}}
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


        valid = is_valid_label(res['images'][label_im_id], label)
        label["valid"]=valid
        res["labels"][label_im_id].append(label)
    return res

def make_cache_key(*args, **kwargs):
    if not request.data:
        return ""
    payload = request.json
    if payload == None:
        return ""
    opts, errors = validate_filter_opts(payload)
    res = json.dumps({**opts, **errors} , sort_keys=True, separators=(',', ':'))
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

def get_survey_flights_dict():
    flights=db.session.query(NOAAImage.flight, NOAAImage.survey, NOAAImage.cam_position).distinct().all()
    d = {}
    for f,s,p in flights:
        if s not in d:
            d[s] = {}
        if not f in d[s]:
            d[s][f] = []
        d[s][f].append(p)
    for s in d:
        for f in d[s]:
            d[s][f].sort()


    return d

