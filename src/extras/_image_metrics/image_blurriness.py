import cv2
from sqlalchemy import not_
from sqlalchemy.orm import joinedload

from noaadb import Session
from noaadb.schema.models import *
from sewar.full_ref import uqi
s = Session()
# TODO make a bluriness metric for images
blurry = ['CHESS2016_N94S_FL12_P__20160421225442.627GMT', 'CHESS2016_N94S_FL21_P__20160513203651.536GMT']
blurred_ims = []
for b in blurry:
    blur_im = s.query(EOImage).filter(EOImage.filename.ilike('%'+b+'%')).first()
    blurred_ims.append(blur_im)

for db_im in blurred_ims:
    im = db_im.ocv_load()
    gray = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)
    x=1
images = s.query(EOImage) \
    .join(TrainTestValid, EOImage.event_key == TrainTestValid.eo_event_key) \
    .filter(TrainTestValid.eo_event_key != None).all()#.filter(EOImage.event_key.ilike('%CHESS2016_N94S_FL22_P__20160517025255%')).all()



# 0.00031804780000903245
# 13.845803346047413
s.close()