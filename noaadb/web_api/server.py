import json
import os

from flask import request, Flask
from flask_cache import Cache
from flask_marshmallow import Marshmallow
from flask_sqlalchemy import SQLAlchemy
from flask_swagger_ui import get_swaggerui_blueprint
from gevent.pywsgi import WSGIServer
from werkzeug.exceptions import BadRequest, abort, default_exceptions
from flask_cors import CORS
from noaadb import DATABASE_URI
from noaadb.schema.models import *


app = Flask(__name__)
CORS(app)
# Configure the SQLAlchemy part of the app instance
app.config['SQLALCHEMY_ECHO'] = False
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False  # TODO signalling support
app.config["DEBUG"] = False if not "DEBUG" in os.environ else bool(int(os.environ["DEBUG"]))
cache = Cache(app,config={'CACHE_TYPE': 'filesystem' if not app.config["DEBUG"] else 'null', 'CACHE_DIR': '/tmp'})


# Initialize Marshmallow
ma = Marshmallow(app)

swagger_blueprint = get_swaggerui_blueprint(
    '/api',
    '/static/swagger.yaml',
    config={
        'app_name': "NOAADB API"
    }
)
app.register_blueprint(swagger_blueprint, url_prefix='/api')

default_exceptions[400] = BadRequest
db = SQLAlchemy(app)
@app.errorhandler(400)

def uflr(e):
    return e, 400

def get_labels_for_image(image):
    label_type = IRLabelEntry if isinstance(image, IRImage) else EOLabelEntry
    labels = db.session.query(label_type).filter(label_type.image_id == image.file_name).all()
    return labels

@app.route("/annotator/image", methods=['GET'])
@cache.cached(timeout=300)
def image():
    image_id = request.args.get('image_id', default=None, type=str)

    image = db.session.query(EOImage).filter(EOImage.file_name == image_id).first()
    if image is None:
        image = db.session.query(IRImage).filter(IRImage.file_name == image_id).first()

    if not image:
        abort(400, "Image name not found %s." % str(image_id))


    labels = get_labels_for_image(image)
    res = \
        {
            'image_id': image.file_name,
            'w': image.width,
            'h': image.height,
            'c': image.depth,
            'filepath': image.file_path,
            'labels': []
        }
    for l in labels:
        l_dict = \
            {
                'guid': l.id,
                'x1': l.x1,
                'x2': l.x2,
                'y1': l.y1,
                'y2': l.y2,
                'label': l.species.name
            }
        res['labels'].append(l_dict)
    return json.dumps(res)


@app.route("/annotator/label", methods=['GET'])
def label():
    guid = request.args.get('guid', default=None, type=int)

    label = db.session.query(IRLabelEntry, IRImage).filter(IRLabelEntry.id == guid)\
        .join(IRImage).first()
    if label is None:
        label = db.session.query(EOLabelEntry, EOImage).filter(EOLabelEntry.id == guid)\
            .join(EOImage).first()

    if not label:
        abort(400, "Label_id not found %d." % guid)

    image = label[1]
    labels = get_labels_for_image(image)

    res = \
        {
            'image_id': image.file_name,
            'w': image.width,
            'h': image.height,
            'c': image.depth,
            'filepath': image.file_path,
            'labels': []
        }
    for l in labels:
        l_dict = \
            {
                'guid': l.id,
                'x1': l.x1,
                'x2': l.x2,
                'y1': l.y1,
                'y2': l.y2,
                'label': l.species.name
            }
        res['labels'].append(l_dict)
    return json.dumps(res)

def main():
    http_server = WSGIServer(('', 5000), app)
    http_server.serve_forever()

if __name__ == "__main__":
    main()