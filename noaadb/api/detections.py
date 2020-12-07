import json
import os
from typing import List

import pymongo
from pymongo.results import InsertOneResult
from thebook.darknet.config_models import parse_model_config
from thebook.vision.models import LabeledImage

# this api does not interact with the postgres db but instead with mongodb and setting the MONGODB_CONN environment variable
# is required to use any of these functions

DATABASE = 'noaadb'
DETECTIONS_COLLECTION = 'detections'
MODEL_COLLECTION = 'models'

# helper functions
def _get_conn_uri():
    return os.environ['MONGODB_CONN']


def _get_db():
    client = pymongo.MongoClient(_get_conn_uri())
    db = client[DATABASE]
    return db

def model_exists(model: str):
    db = _get_db()
    c = db[MODEL_COLLECTION]
    return c.count_documents({ 'model': model }, limit = 1) != 0

# mongo setup functions
def set_indexes():
    db = _get_db()
    c = db[DETECTIONS_COLLECTION]
    c.create_index([("model", pymongo.ASCENDING), ("unique", 1)])

    db = _get_db()
    c = db[MODEL_COLLECTION]
    c.create_index([("model", pymongo.ASCENDING), ("unique", 1)])

# API functions
# detections
def register_detection_set(model: str, dets: List[LabeledImage]) -> InsertOneResult:
    db = _get_db()
    c = db[DETECTIONS_COLLECTION]
    if not model_exists(model):
        raise Exception('Model %s is not registered' % model)
    record = {'model': model, 'detections': [d.to_dict() for d in dets]}
    return c.insert_one(record)

def get_detection_set(model: str) -> List[LabeledImage]:
    db = _get_db()
    c = db[DETECTIONS_COLLECTION]
    res = c.find({'model': model})
    if len(res) == 0:
        return None
    x = res[0]
    dets = [LabeledImage.from_dict(x) for x in x['detections']]
    return dets

def list_detection_sets() -> List[str]:
    db = _get_db()
    c = db[DETECTIONS_COLLECTION]
    documents = list(c.find({}, {'model':1}))
    return documents

def register_detection_set_file(model: str, filepath: str) -> InsertOneResult:
    with open(filepath, 'r') as f:
        dict_arr = json.loads(f.read())
    dets = [LabeledImage.from_dict(d) for d in dict_arr]
    return register_detection_set(model, dets)


# model
def register_model(model: str, model_fp: str) -> InsertOneResult:
    if model_exists(model):
        raise Exception('Model exists')
    db = _get_db()
    c = db[MODEL_COLLECTION]
    cfg = parse_model_config(model_fp)
    record = {'model': model, 'cfg': cfg}
    return c.insert_one(record)

