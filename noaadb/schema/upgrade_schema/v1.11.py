from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy import DDL, func
from noaadb import DATABASE_URI
from noaadb.schema.models import NOAAImage, ImageType, Sighting, LabelChips, Chip, ImageDimension, Species, \
    FPChips, LabelEntry, EOLabelEntry, FalsePositiveSightings, TruePositiveSighting

from scripts.util import printProgressBar



engine = create_engine(DATABASE_URI, echo=False)

# create session
Session = sessionmaker(bind=engine)
session = Session()

chip_size = 832
#
chip = aliased(Chip)
# chip_subq = session.query(LabelChips).join(LabelChips.chip).filter(and_(chip.width == chip_size)).subquery()
# chip_alias = aliased(LabelChips, chip_subq)
#
label = aliased(EOLabelEntry)
chip_size_subq = session.query(LabelChips, chip_id.label('chip_id')).filter(and_(chip.width == chip_size))\
    .outerjoin(LabelChips, Chip.id == LabelChips.chip_id).subquery()

# labels_per_chip_per_image = \
#     .join(label, LabelChips.label)\
#     .group_by(label.image_id, LabelChips.chip_id).all()
labels_per_chip_per_image = session.query(func.count(LabelChips.chip_id), LabelChips.chip_id, label.image_id)\
    .join(LabelChips.chip_id == chip_size_subq.chip_id).filter(and_(chip.width == chip_size))\
    .join(label, LabelChips.label)\
    .group_by(label.image_id, LabelChips.chip_id).all()

labels_per_image = session.query(func.count(LabelChips.label_id), label.image_id) \
    .join(LabelChips.chip).filter(and_(chip.width == chip_size)) \
    .join(label, LabelChips.label).distinct(LabelChips.label_id).group_by(label.image_id, LabelChips.label_id).all()

labels_per_chip_per_image_dict = {}
for num_in_chip, chip_id, image_id in labels_per_chip_per_image:
    if image_id not in labels_per_chip_per_image_dict:
        labels_per_chip_per_image_dict[image_id] = []
    labels_per_chip_per_image_dict[image_id].append({'chip_id':chip_id, 'ct': num_in_chip})

labels_per_image_dict = {}
for label_ct, image in labels_per_image:
    labels_per_image_dict[image] = label_ct

assert (len(labels_per_chip_per_image_dict) == len(labels_per_image_dict))

final_chips_by_image_dict = {}
for image in labels_per_image_dict.keys():
    label_ct = labels_per_image_dict[image]
    chip_counts = labels_per_chip_per_image_dict[image]
    ct_in_chips = sum([c['ct'] for c in chip_counts])
    assert label_ct <= ct_in_chips # assert no labels missing in chips
    if label_ct == ct_in_chips:
        # no duplicates so we are good
        final_chips_by_image_dict[image] = chip_counts
        continue
    x=1
b=11