import json
import time
from _operator import and_

from gevent.pywsgi import WSGIServer
from flask import request, render_template
from sqlalchemy import not_, or_, alias, func
from sqlalchemy.orm import aliased
from werkzeug.exceptions import BadRequest, abort, default_exceptions

from noaadb.web_api.config import *
from noaadb.web_api.ml.ml_util import get_iou
from noaadb.web_api.server_utils import validate_label_filter_opts, get_all_images, combind_images_labels_to_json, \
    make_hotspots_cache_key, \
    get_jobs_dict, get_workers_dict, get_species_dict, default_label_filter_options, get_survey_flights_dict, labels_query, \
    make_array_cache_key, false_positive_query
from noaadb.web_api.models import *
from noaadb.schema.models import Chip, LabelChips, ImageDimension

default_exceptions[400] = BadRequest

@app.errorhandler(400)
def uflr(e):
    return e, 400

@app.route("/api/ml/nms", methods=['POST'])
@cache.cached(timeout=300, key_prefix=make_hotspots_cache_key)
def ml_nms():
    image_name = request.args.get('image_name', default=None, type=str)

    image = db.session.query(NOAAImage).filter(NOAAImage.file_name == image_name).first()
    if not image:
        abort(400, "Image name not found %s." % str(image_name))

    if not request.data: abort(400, "Empty request is invalid.") # EMPTY  REQUEST ERROR
    if request.json is None: abort(400, "Empty request is invalid.")  # EMPTY  REQUEST ERROR
    if not isinstance(request.json, list): abort(400, "Request is not a list.")
    bounding_boxes = request.json # list of {x1: int, y1: int, x2: int, y2: int, class_name: string} objects

    labels = db.session.query(TruePositiveLabels).filter(TruePositiveLabels.image == image).join(TruePositiveLabels.species).all()
    # spec = aliased(Species)
    # eo_image = aliased(NOAAImage)
    # labels = db.session.query(Label).join(spec, Label.species).filter(spec.name != 'false positive').join(eo_image, Label.image).filter_by(survey='test_kotz_2019').all()
    matches = [None] * len(bounding_boxes) # list of corresponding labels, or None if no matches
    # check each bounding box given against database labels
    for label in labels:
        for i, bb_in in enumerate(bounding_boxes):
            bb_gt = {"x1": label.x1,"y1": label.y1,"x2": label.x2,"y2": label.y2}
            iou = get_iou(bb_in, bb_gt)
            if iou > 0:
                if matches[i] is None:
                    matches[i] = [label, iou]
                elif matches[i][1] < iou:
                    matches[i] = [label, iou]
    x=1
    res = []
    for match in matches:
        if match is None:
            res.append(None)
            continue
        res.append({'match':label_schema.dump(match[0]), 'species': specie_schema.dump(match[0].species), 'iou': match[1]})
    return json.dumps(res)

@app.route("/api/hotspots", methods=['GET', 'POST'])
@cache.cached(timeout=300, key_prefix=make_hotspots_cache_key)
def hotspots_filter():
    if not request.data:
        abort(400, "Empty request is invalid.") # EMPTY  REQUEST ERROR
    payload = request.json
    if payload is None:
        abort(400, "Empty request is invalid.")  # EMPTY  REQUEST ERROR
    opts, errors = validate_label_filter_opts(payload)
    if errors:
        abort(400, json.dumps(errors,indent=2))

    query=labels_query(opts)
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

@app.route("/api/image", methods=['GET'])
@cache.cached(timeout=300)
def image():
    image_name = request.args.get('image_name', default=None, type=str)
    image = db.session.query(NOAAImage).filter(NOAAImage.file_name == image_name).first()
    if not image:
        abort(400, "Image name not found %s." % str(image_name))
    return json.dumps(image_schema.dump(image))


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

@app.route("/api/image_chips", methods=['GET'])
@cache.cached(timeout=300)
def image_chips():
    width = request.args.get('w', default=608, type=int)
    height = request.args.get('h', default=608, type=int)
    image_name = request.args.get('image_name', default=None, type=str)

    image = db.session.query(NOAAImage).filter(NOAAImage.file_name==image_name).first()
    if not image:
        abort(400, "Image name not found %s." % str(image_name))

    w = image.width
    h = image.height

    im_dim = db.session.query(ImageDimension).filter(and_(ImageDimension.width == w, ImageDimension.height == h)).first()
    if not im_dim:
        abort(400, "Image dimension %dx%d" % (w,h))

    chips = db.session.query(Chip).filter(and_(and_(Chip.width==width, Chip.height == height), Chip.image_dimension == im_dim)).all()
    chips_dict = chips_schema.dump(chips)
    return json.dumps(chips_dict)

@app.route("/api/false_positives", methods=['GET'])
@cache.cached(timeout=300)
def false_positive_chips():
    width = request.args.get('w', default=608, type=int)
    height = request.args.get('h', default=608, type=int)
    # fp under .5 confidence may not have been human reviewed
    confidence = request.args.get('confidence', default=.5, type=float)
    unique_dimensions = db.session.query(Chip.width, Chip.height).distinct().all()
    if not (width,height) in unique_dimensions:
        res = {}
        res['available_dimensions'] = unique_dimensions
        res['message'] = "The width and height provided are not available, if you need something specific contact yuval@uw.edu."
        return json.dumps(res)

    q =false_positive_query(confidence)
    chips=q.all()
    chips_dict = {}
    im_dict = {}
    id = -1
    for r in chips:
        if not r.image_id in chips_dict:
            chips_dict[r.image_id] = []
            im_dict[r.image_id] = r.image

        chips_dict[r.image_id].append(r.chip)

    for k in im_dict:
        im_dict[k] = image_schema.dump(im_dict[k])
    for k in chips_dict:
        chips_dict[k] = chips_schema.dump(chips_dict[k])

    d = {"images":im_dict,
         "chips": chips_dict}
    return json.dumps(d)

# TODO CHIPS BY IMAGE IDv
@app.route("/api/chips", methods=['POST'])
@cache.cached(timeout=300, key_prefix=make_array_cache_key)
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
    query = db.session.query(LabelChips) \
        .filter(LabelChips.label_id.in_(payload)) \
        .join(Chip) \
        .filter(and_(Chip.width == width, Chip.height == height))
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