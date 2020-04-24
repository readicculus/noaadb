import json
import time
from _operator import and_

from gevent.pywsgi import WSGIServer
from flask import request, render_template
from sqlalchemy import not_, or_
from sqlalchemy.orm import aliased
from werkzeug.exceptions import BadRequest, abort, default_exceptions

from noaadb.api.config import *
from noaadb.api.server_utils import validate_filter_opts, get_all_images, combind_images_labels_to_json, make_cache_key, \
    get_jobs_dict, get_workers_dict, get_species_dict, default_filter_options, get_survey_flights_dict, hotspots_query
from noaadb.api.models import *
from noaadb.schema.models import Chip, LabelChips

default_exceptions[400] = BadRequest

@app.errorhandler(400)
def uflr(e):
    return e, 400

@app.route("/api/hotspots", methods=['GET', 'POST'])
@cache.cached(timeout=300, key_prefix=make_cache_key)
def hotspots_filter():
    if not request.data:
        abort(400, "Empty request is invalid.") # EMPTY  REQUEST ERROR
    payload = request.json
    if payload is None:
        abort(400, "Empty request is invalid.")  # EMPTY  REQUEST ERROR
    opts, errors = validate_filter_opts(payload)
    if errors:
        abort(400, json.dumps(errors,indent=2))

    query=hotspots_query(opts)
    start = time.time()
    labels = query.all()
    print("Query time: %.4f" % (time.time() - start))
    # since we sort query by image id we can go through them once and create a unique list of image ids
    unique_ims = []
    id = -1
    for r in labels:
        if r.image_id != id:
            id = r.image_id
            unique_ims.append(r.image_id)

    images = get_all_images(unique_ims)
    res = combind_images_labels_to_json(images, labels)

    return json.dumps({"images":res["images"],
                       "labels": res["labels"],
                       # "invalid": res["invalid"],
                       # "jobs": get_jobs_dict(),
                       # "species": get_species_dict(),
                       # "workers": get_workers_dict(),
                       "image_count":len(images),
                       "label_count":len(labels)})

@app.route("/api/species")
@cache.cached(timeout=300)
def species():
    return json.dumps(get_species_dict())

@app.route("/api/workers")
@cache.cached(timeout=300)
def workers():
    return json.dumps(get_workers_dict())

@app.route("/api/jobs")
@cache.cached(timeout=300)
def jobs():
    return json.dumps(get_jobs_dict())

@app.route("/api/surveys")
@cache.cached(timeout=300)
def surveys():
    return json.dumps(get_survey_flights_dict())


@app.route("/api/chips", methods=['POST'])
@cache.cached(timeout=300)
def chips_by_id():
    width = request.args.get('w', default=-1, type=int)
    height = request.args.get('h', default=-1, type=int)
    unique_dimensions = db.session.query(Chip.width, Chip.height).distinct().all()
    if not (width,height) in unique_dimensions:
        res = {}
        res['available_dimensions'] = unique_dimensions
        res['message'] = "The width and height provided are not available, if you need something specific contact yuval@uw.edu."
        return json.dumps(res)

    if not request.data: abort(400, "Empty request is invalid.") # EMPTY  REQUEST ERROR
    if request.json is None: abort(400, "Empty request is invalid.")  # EMPTY  REQUEST ERROR
    if not isinstance(request.json, list): abort(400, "Request is not a list.")

    payload = request.json
    # get all label-chip pairs for the given label ids and the given chip dimensions
    query = db.session.query(LabelChips)\
        .filter(LabelChips.label_id.in_(payload))\
        .join(Chip)\
        .filter(and_(Chip.width==width, Chip.height==height))
    qr = query.all()
    chips = {}
    relative_loc = {}
    for label_chip in qr:
        if not label_chip.label.image_id in relative_loc:
            relative_loc[label_chip.label.image_id] = {}
        if label_chip.label_id not in relative_loc[label_chip.label.image_id]:
            relative_loc[label_chip.label.image_id][label_chip.label_id] = []
        relative_loc[label_chip.label.image_id][label_chip.label_id].append(label_chip)

        if not label_chip.chip_id in chips:
            chips[label_chip.chip_id] = chip_schema.dump(label_chip.chip)

    for k in relative_loc:
        for j in relative_loc[k]:
            relative_loc[k][j] = chiphotspots_schema.dump(relative_loc[k][j])
    return json.dumps({"imageid_to_labelid_to_chipid":relative_loc, "chips_by_id": chips})

def main():
    http_server = WSGIServer(('', 5000), app)
    http_server.serve_forever()

if __name__ == "__main__":
    main()