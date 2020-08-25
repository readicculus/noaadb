import random

from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy import DDL
from noaadb import DATABASE_URI
from noaadb.web_api.server_utils import labels_query
from noaadb.schema.models import TrainTestSplit, MLType

engine = create_engine(DATABASE_URI, echo=False)

engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS ml"))

TRAIN_SPLIT=.8

TrainTestSplit.__table__.drop(bind=engine, checkfirst=True)
TrainTestSplit.__table__.create(bind=engine)
filter_options = \
    {
      "species_list": [
        "Ringed Seal",
        "Bearded Seal",
        "Polar Bear"
      ],
      "image_type": "eo",
      "show_shadows": False,
      "workers": [],
      "jobs": [],
      "surveys": [],
      "flights": [],
      "camera_positions": [],
      "show_removed_labels": False,
      "ml_data_type": "all"
    }
q = labels_query(filter_options)
labels = q.all()
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
train_ids = []
test_ids = []

im_keys = list(image_label_dict.keys())
random.shuffle(im_keys)
test_im_keys = []
for idx, im_id in enumerate(im_keys):
    labels = image_label_dict[im_id]
    for label in labels:
        train_ids.append(label.id)
        train_species_count[label.species_id] += 1

    done=False
    for k in train_species_count:
        if train_species_count[k] > target[k]:
            done = True

    if done:
        test_im_keys = im_keys[idx:]
        break

for idx, im_id in enumerate(test_im_keys):
    labels = image_label_dict[im_id]
    for label in labels:
        test_ids.append(label.id)
        test_species_count[label.species_id] += 1


for k in train_species_count:
    print("%d %.4f Train" % (k, train_species_count[k]/total_species_count[k]))

Session = sessionmaker(bind=engine)
session = Session()
x=1

for idx, id in enumerate(train_ids):
    tts = TrainTestSplit(
        label_id=id,
        type=MLType.TRAIN
    )
    session.add(tts)
    if idx % 50 == 0:
        session.commit()
session.commit()
for idx, id in enumerate(test_ids):
    tts = TrainTestSplit(
        label_id=id,
        type=MLType.TEST
    )
    session.add(tts)
    if idx % 50 == 0:
        session.commit()
session.commit()