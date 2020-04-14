import json
import time
from gevent.pywsgi import WSGIServer
from flask import request
from sqlalchemy import not_, or_
from sqlalchemy.orm import aliased

from noaadb.api.config import *
from noaadb.api.server_utils import validate_filter_opts, get_all_images, combind_images_labels_to_json, make_cache_key, \
    get_jobs_dict, get_workers_dict, get_species_dict
from noaadb.api.models import *

@app.route("/api/hotspots", methods=['GET', 'POST'])
@cache.cached(timeout=300, key_prefix=make_cache_key)
def hotspots_filter():
    payload = request.json
    opts = validate_filter_opts(payload)

    species = aliased(Species)

    query = db.session.query(Label)
    # TODO: use filter to filter color and another pararmeter to filter hotspots only
    if opts["image_type"] == "ir":  # IR Image
        query = query.join(Hotspot, Hotspot.ir_label_id == Label.id)
    else:                           # EO Image
        query = query.join(Hotspot, Hotspot.eo_label_id == Label.id)

    if not opts["show_shadows"]:
        query = query.filter(not_(Label.is_shadow))
    if not opts["show_removed"]:
        query = query.filter(Label.end_date == None)

    invalid_filter_pre_join = or_(
            Worker.name == 'noaa',  # original noaa label that I haven't looked at yet
            Label.end_date != None,  # end_date exists if label was removed
            Label.x1 < 0,  # out of bounds
            Label.y1 < 0,  # out of bounds
            Label.x1 == None,
            Label.x2 == None,
            Label.y1 == None,
            Label.y2 == None
        )
    if opts["invalid_labels_only"] or opts["exclude_invalid_labels"]:
        if opts["invalid_labels_only"]:
            query = query.filter(invalid_filter_pre_join)
        elif opts["exclude_invalid_labels"]:
            query = query.filter(not_(invalid_filter_pre_join))

        query = query.join(NOAAImage, Label.image)
        invalid_filter_post_join = or_(
            Label.x2 > NOAAImage.width,  # out of bounds
            Label.y2 > NOAAImage.height  # out of bounds
        )
        if opts["invalid_labels_only"]:
            query = query.filter(invalid_filter_post_join)
        elif opts["exclude_invalid_labels"]:
            query = query.filter(not_(invalid_filter_post_join))

    query = query.join(species, Label.species).filter(species.name.in_(opts["species_list"]))
    query = query.join(Worker, Label.worker) \
                 .join(Label.job)

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
                       "invalid": res["invalid"],
                       "jobs": get_jobs_dict(),
                       "species": get_species_dict(),
                       "workers": get_workers_dict(),
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