import cv2
import numpy as np
from flask import request, Response, abort

from noaadb.schema.models import *
from noaadb.web_api.server import app, db


def min_max_norm(im):
    im_ir = ((im - np.min(im)) / (0.0 + np.max(im) - np.min(im)))
    im_ir = im_ir*255.0
    im_ir = im_ir.astype(np.uint8)
    return im_ir

def read_ir_norm(fp):
    im = cv2.imread(fp, cv2.IMREAD_ANYDEPTH)
    im_norm = min_max_norm(im)
    return im_norm

def load_image(record):
    type = 'ir' if isinstance(record, IRImage) else 'eo'

    # normalize and return ir image
    if type == 'ir':
        im = read_ir_norm(record.file_path)
        retval, jpg = cv2.imencode('.jpg', cv2.cvtColor(im, cv2.COLOR_GRAY2RGB))
        return Response(b'--frame\r\n' b'Content-Type: image/jpeg\r\n\r\n' + jpg.tobytes() + b'\r\n\r\n', mimetype='multipart/x-mixed-replace; boundary=frame')

    # otherwise return eo image
    im = cv2.imread(record.file_path)
    return im

@app.route('/annotator/get_image', methods=['GET'])
def get_image():
    image_id = request.args.get('image_id', default=None, type=str)

    image = db.session.query(EOImage).filter(EOImage.file_name == image_id).first()

    if image is None:
        image = db.session.query(IRImage).filter(IRImage.file_name == image_id).first()

    if not image:
        abort(400, "Image name not found %s." % str(image_id))

    im = load_image(image)

    # labels = get_labels_for_image(image)

    retval, jpg = cv2.imencode('.jpg', im)
    return Response(b'--frame\r\n' b'Content-Type: image/jpeg\r\n\r\n' + jpg.tobytes() + b'\r\n\r\n',
                    mimetype='multipart/x-mixed-replace; boundary=frame')



