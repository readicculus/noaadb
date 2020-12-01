import random

from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy import DDL
from noaadb import DATABASE_URI
from noaadb.api.query_builder import qb_image_with_sightings, qb_labels
from noaadb.schema.models.ml_data import TrainTestSplit, MLType
from noaadb import Session
engine = create_engine(DATABASE_URI, echo=False)

engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS ml"))

TRAIN_SPLIT=.8

TrainTestSplit.__table__.drop(bind=engine, checkfirst=True)
TrainTestSplit.__table__.create(bind=engine)
s = Session()

labels_q = qb_labels(type='ir')
labels = labels_q.with_session(s).all()
total_species_count = {}
image_label_dict = {}

for idx, r in enumerate(labels):
    # calculate total species counts
    if not r.species_id in total_species_count:
        total_species_count[r.species_id] = 0
    total_species_count[r.species_id] += 1

    # organize by image id
    label_im_id = r.image_id
    if not label_im_id in image_label_dict:
        image_label_dict[label_im_id] = []
    image_label_dict[label_im_id].append(r)


target = {x:total_species_count[x]*TRAIN_SPLIT for x in total_species_count.keys()}
train_species_count = {x:0 for x in total_species_count.keys()}
test_species_count = {x:0 for x in total_species_count.keys()}

im_keys = list(image_label_dict.keys())
random.shuffle(im_keys)
test_im_keys = []
train_im_keys = []
for idx, im_id in enumerate(im_keys):
    # check if done

    ct = 0
    for k in train_species_count:
        if train_species_count[k] > target[k]:
            ct += 1
    if ct >= 3:
        test_im_keys = im_keys[idx:]
        break


    labels = image_label_dict[im_id]
    for label in labels:
        train_species_count[label.species_id] += 1
    train_im_keys.append(im_id)

for idx, im_id in enumerate(test_im_keys):
    labels = image_label_dict[im_id]
    for label in labels:
        test_species_count[label.species_id] += 1


for k in train_species_count:
    print("%d %.4f Train" % (k, train_species_count[k]/total_species_count[k]))


x=1

for idx, id in enumerate(train_im_keys):
    tts = TrainTestSplit(
        image_id=id,
        type=MLType.TRAIN
    )
    s.add(tts)
    if idx % 500 == 0:
        s.commit()
s.commit()
for idx, id in enumerate(test_im_keys):
    tts = TrainTestSplit(
        image_id=id,
        type=MLType.TEST
    )
    s.add(tts)
    if idx % 500 == 0:
        s.commit()
s.commit()
s.close()