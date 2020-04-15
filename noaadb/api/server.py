import json
import time
from gevent.pywsgi import WSGIServer
from flask import request, render_template
from sqlalchemy import not_, or_
from sqlalchemy.orm import aliased
from werkzeug.exceptions import BadRequest, abort, default_exceptions

from noaadb.api.config import *
from noaadb.api.server_utils import validate_filter_opts, get_all_images, combind_images_labels_to_json, make_cache_key, \
    get_jobs_dict, get_workers_dict, get_species_dict, default_filter_options
from noaadb.api.models import *



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

    species = aliased(Species)

    query = db.session.query(Label)
    if opts["image_type"] == "ir":  # IR Image
        query = query.join(Hotspot, Hotspot.ir_label_id == Label.id)
    else:                           # EO Image
        query = query.join(Hotspot, Hotspot.eo_label_id == Label.id)

    if not opts["show_shadows"]:
        query = query.filter(not_(Label.is_shadow))
    if not opts["show_removed_labels"]:
        query = query.filter(Label.end_date == None)


    # if opts["exclude_invalid_labels"]:
    #     # If we exclude invalid labels it is a bit slower because we have to join with Image table
    #     # to ensure it is in image bounds
    #     invalid_filter_pre_join = or_(
    #         Label.end_date != None,  # end_date exists if label was removed
    #         Label.x1 < 0,  # out of bounds
    #         Label.y1 < 0,  # out of bounds
    #         Label.x1 == None,
    #         Label.x2 == None,
    #         Label.y1 == None,
    #         Label.y2 == None
    #     )
    #     query = query.filter(not_(invalid_filter_pre_join))
    #     query = query.join(NOAAImage, Label.image)
    #     invalid_filter_post_join = or_(
    #         Label.x2 > NOAAImage.width,  # out of bounds
    #         Label.y2 > NOAAImage.height  # out of bounds
    #     )
    #     query = query.filter(not_(invalid_filter_post_join))

    # Filter species
    query = query.join(species, Label.species)
    if len(opts["species_list"]) > 0:
        query = query.filter(species.name.in_(opts["species_list"]))

    query = query.join(Worker, Label.worker)
    # Filter workers
    if len(opts["workers"]) > 0:
        query = query.filter(Worker.name.in_(opts["workers"]))

    query = query.join(Job, Label.job)
    # Filter jobs
    if len(opts["jobs"]) > 0:
        query = query.filter(Job.job_name.in_(opts["jobs"]))

    query.order_by(Label.image_id)

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

def main():
    http_server = WSGIServer(('', 5000), app)
    http_server.serve_forever()

if __name__ == "__main__":
    main()