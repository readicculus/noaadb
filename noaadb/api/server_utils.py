import json
import hashlib
import time

from flask import request
from sqlalchemy import not_, or_, and_
from sqlalchemy.orm import aliased

from noaadb.api.config import db
from noaadb.api.models import label_schema, images_schema, labels_schema, jobs_schema, workers_schema, species_schema
from noaadb.schema.models import NOAAImage, Job, Worker, Species, TruePositiveLabels, EOIRLabelPair, TrainTestSplit, MLType, ImageType, \
    FPChips, Chip

default_label_filter_options = \
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
      "show_removed_labels": False,
      "ml_data_type": "all"
    }


def validate_label_filter_opts(req):
    filter_params = {}
    errors = {}
    for k in default_label_filter_options:
        if not k in req:
            filter_params[k] = default_label_filter_options[k]
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
            elif k == "ml_data_type":
                if not req[k] in ["train", "test", "all"]:
                    errors[k] = "Invalid ml_data_type %s, following are accepted options ['train', 'test', 'all']." % req[k]
                else:
                    filter_params[k] = req[k]
            else:
                filter_params[k] = req[k]
    return filter_params, errors

def get_all_images(unique_image_ids):
    images = db.session.query(NOAAImage).options(db.defer('meta')).filter(NOAAImage.id.in_(unique_image_ids)).all()
    return images


def false_positive_query(confidence, session=db.session, width=608, height=608):
    chip = aliased(Chip)
    query = session.query(FPChips)
    query = query.join(chip, FPChips.chip_id == chip.id)
    query = query.filter(and_(chip.width == width,chip.height == height))
    query = query.join(FPChips.label)
    if confidence != 0:
        query = query.filter(TruePositiveLabels.confidence > confidence)
    query = query.join(NOAAImage, TruePositiveLabels.image)
    query = query.filter(NOAAImage.type == ImageType.EO)
    query.order_by(TruePositiveLabels.image_id)
    return query

def hs_query(opts):
    pass

def labels_query(opts):
    species = aliased(Species)

    query = db.session.query(TruePositiveLabels)
    if opts["image_type"] == "ir":  # IR Image
        query = query.join(EOIRLabelPair, EOIRLabelPair.ir_label_id == TruePositiveLabels.id)
    else:  # EO Image
        query = query.join(EOIRLabelPair, EOIRLabelPair.eo_label_id == TruePositiveLabels.id)

    if not opts["ml_data_type"] == "all":
        type = MLType.TRAIN if opts["ml_data_type"] == "train" else MLType.TEST
        query = query.join(TrainTestSplit, TrainTestSplit.label_id == TruePositiveLabels.id)\
            .filter(TrainTestSplit.type == type)

    if not opts["show_shadows"]:
        query = query.filter(not_(TruePositiveLabels.is_shadow))
    if not opts["show_removed_labels"]:
        query = query.filter(TruePositiveLabels.end_date == None)

    if opts["exclude_invalid_labels"]:
        # If we exclude invalid labels it is a bit slower because we have to join with Image table
        # to ensure it is in image bounds
        invalid_filter_pre_join = or_(
            TruePositiveLabels.end_date != None,  # end_date exists if label was removed
            TruePositiveLabels.x1 < 0,  # out of bounds
            TruePositiveLabels.y1 < 0,  # out of bounds
            TruePositiveLabels.x1 == None,
            TruePositiveLabels.x2 == None,
            TruePositiveLabels.y1 == None,
            TruePositiveLabels.y2 == None
        )
        query = query.filter(not_(invalid_filter_pre_join))
        query = query.join(NOAAImage, TruePositiveLabels.image)
        invalid_filter_post_join = or_(
            TruePositiveLabels.x2 > NOAAImage.width,  # out of bounds
            TruePositiveLabels.y2 > NOAAImage.height  # out of bounds
        )
        query = query.filter(not_(invalid_filter_post_join))

    # Filter species
    query = query.join(species, TruePositiveLabels.species)
    if len(opts["species_list"]) > 0:
        query = query.filter(species.name.in_(opts["species_list"]))

    query = query.join(Worker, TruePositiveLabels.worker)
    # Filter workers
    if len(opts["workers"]) > 0:
        query = query.filter(Worker.name.in_(opts["workers"]))

    query = query.join(Job, TruePositiveLabels.job)
    # Filter jobs
    if len(opts["jobs"]) > 0:
        query = query.filter(Job.name.in_(opts["jobs"]))

    if len(opts["surveys"]) > 0:
        query = query.filter(NOAAImage.survey.in_(opts["surveys"]))

    if len(opts["camera_positions"]) > 0:
        query = query.filter(NOAAImage.cam_position.in_(opts["camera_positions"]))

    if len(opts["flights"]) > 0:
        query = query.filter(NOAAImage.flight.in_(opts["flights"]))

    query.order_by(TruePositiveLabels.image_id)
    return query

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

def make_hotspots_cache_key(*args, **kwargs):
    if not request.data:
        return ""
    payload = request.json
    if payload == None:
        return ""
    opts, errors = validate_label_filter_opts(payload)
    res = json.dumps({**opts, **errors} , sort_keys=True, separators=(',', ':'))
    hash_object = hashlib.sha1(res.encode('utf-8'))
    return hash_object.hexdigest()

def make_array_cache_key(*args, **kwargs):
    if not request.data:
        return ""
    payload = request.json
    if payload == None:
        return ""
    res = json.dumps(payload)
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

