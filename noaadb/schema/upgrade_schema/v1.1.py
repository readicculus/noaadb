from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy import DDL, func
from noaadb import DATABASE_URI
from noaadb.api.server_utils import false_positive_query
from noaadb.schema.models import TruePositiveLabels, NOAAImage, ImageType, Sighting, LabelChips, Chip, ImageDimension, Species, \
    FPChips

from scripts.util import printProgressBar



engine = create_engine(DATABASE_URI, echo=False)

engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS chips"))


LabelChips.__table__.drop(bind=engine, checkfirst=True)
FPChips.__table__.drop(bind=engine, checkfirst=True)
Chip.__table__.drop(bind=engine, checkfirst=True)
ImageDimension.__table__.drop(bind=engine, checkfirst=True)
ImageDimension.__table__.create(bind=engine)
Chip.__table__.create(bind=engine)
FPChips.__table__.create(engine)
LabelChips.__table__.create(engine)

# create session
Session = sessionmaker(bind=engine)
session = Session()

# Populate Chip Table
images = session.query(NOAAImage).filter(NOAAImage.type == ImageType.RGB).all()
# def plot_tiles(d):
#     img = np.zeros((im_h, im_w, 1), dtype="uint8")
#     cv2.rectangle(img, (x1, y1), (x2, y2), (255, 255, 0), 2)
#     scale_percent = 10  # percent of original size
#     width = int(img.shape[1] * scale_percent / 100)
#     height = int(img.shape[0] * scale_percent / 100)
#     dim = (width, height)
#     resized = cv2.resize(img, dim, interpolation=cv2.INTER_AREA)
#     cv2.imshow("ImageWindow", resized)
#     cv2.waitKey()
#     pass
def tile_image(im_w, im_h, c_w, c_h, overlap):
    m = im_w // (c_w - overlap)
    n = im_h // (c_h - overlap)
    m_remainder = im_w - (m*(c_w))
    n_remainder = im_h - (n*(c_h))

    tiles = []
    for i in range(m+1):
        for j in range(n+1):
            x1 = i*(c_w - overlap) if i !=m else im_w - c_w
            x2 = x1 + c_w
            y1 = j*(c_h - overlap) if j !=n else im_h - c_h
            y2 = y1 + c_h
            tiles.append([(x1,y1),(x2,y2)])
    return tiles

image_dims = {}

for image in images:
    key = "%dx%d" % (image.width, image.height)
    if not key in image_dims:
        im_dim = ImageDimension(
            width=image.width,
            height=image.height
        )
        session.add(im_dim)
        image_dims[key] = im_dim
    session.flush()

cw,ch,co = [416,608,832,1248, 1664], [416, 608,832,1248, 1664], [120, 120,120,120]
for k in image_dims:
    dims = image_dims[k]
    print("Image %dx%d:" % (dims.width,dims.height))
    for chip_w, chip_h, chip_overlap in zip(cw,ch,co):
            tiles = tile_image(dims.width,dims.height, chip_w, chip_h, chip_overlap)
            print(" %d %dx%d chips" % (len(tiles), chip_w, chip_h))

            for tile in tiles:
                c = Chip(
                    image_dimension=dims,
                    width=chip_w,
                    height=chip_h,
                    overlap=chip_overlap,
                    x1=tile[0][0],
                    y1=tile[0][1],
                    x2=tile[1][0],
                    y2=tile[1][1]
                )
                session.add(c)
    session.commit()
    session.flush()


def percent_on(chip, label):

    dx = min(chip.x2, label.x2) - max(chip.x1, label.x1)
    dy = min(chip.y2, label.y2) - max(chip.y1, label.y1)
    if (dx < 0) and (dy < 0):
        return None
    intersected_area = dx * dy
    label_area = (label.x2 - label.x1) * (label.y2 - label.y1)
    return intersected_area / label_area

def assign_chips_for_labels(labels):
    total = len(labels)
    added = 0
    for i,label in enumerate(labels):

        im_dim = image_dims[key]
        chips_containing_partial__label = session.query(Chip).filter(Chip.image_dimension_id == im_dim.id) \
            .filter(and_(Chip.x1 <= label.x2, label.x1 <= Chip.x2,Chip.y1 <= label.y2, label.y1 <= Chip.y2)) \
            .all()
        added += len(chips_containing_partial__label)
        for chip in chips_containing_partial__label:
            ch = LabelChips(label=label, chip=chip, image=label.image, percent_intersection=percent_on(chip,label))
            session.add(ch)
        if i % 100 == 0:
            printProgressBar(i, total, prefix='Progress:', suffix=' - added %d rows' % (added), length=70, printEnd="")
        if i % 500 == 0:
            session.commit()
    session.commit()

def assign_chips_for_fps(fps):
    chips_with_labels = session.query(LabelChips.chip_id, LabelChips.image_id).distinct().all()
    im_chips_used = {}
    for cid,imid in chips_with_labels:
        if imid not in im_chips_used:
            im_chips_used[imid] = []
        im_chips_used[imid].append(cid)
    total = len(fps)
    added = 0
    image_has_legit_labels = 0
    for i, fp in enumerate(fps):
        label_already_in_image_chips = im_chips_used[fp.image.id] if fp.image.id in im_chips_used else []
        im_w = fp.image.width
        im_h = fp.image.height
        chips_fully_containing_fp = session.query(Chip)\
            .filter(Chip.image_dimension_id == im_dim.id) \
            .filter(and_(Chip.x1 <= fp.x1, fp.x2 <= Chip.x2,Chip.y1 <= fp.y1, fp.y2 <= Chip.y2))

        if len(label_already_in_image_chips) > 0:
            image_has_legit_labels+=1
            chips_fully_containing_fp = chips_fully_containing_fp.filter(~Chip.id.in_(label_already_in_image_chips))

        chips_fully_containing_fp = chips_fully_containing_fp.all()

        added += len(chips_fully_containing_fp)
        chip_sizes_added = []
        for chip in chips_fully_containing_fp:
            if chip.width in chip_sizes_added:
                continue  # only one entry per chip size per fp
            chip_sizes_added.append(chip.width)
            ch = FPChips(label=fp, chip=chip, image=fp.image, percent_intersection=percent_on(chip,fp))
            session.add(ch)
        if i % 100 == 0:
            printProgressBar(i, total, prefix='Progress:', suffix='%d/%d Complete - added %d rows - %d in ims w/legit labels' % (i, total, added, image_has_legit_labels), length=70, printEnd="")
        if i % 500 == 0:
            session.commit()
        x=1
    session.commit()


# Populate ChipHotspot Table
species = aliased(Species)
query = session.query(TruePositiveLabels)
query = query.join(species, species.id == TruePositiveLabels.species_id)
query = query.filter(species.name != 'false positive')
query = query.join(NOAAImage, TruePositiveLabels.image)
query = query.filter(NOAAImage.type == ImageType.RGB)
labels = query.all()
# fp_labels = false_positive_query(0.0, session=session).all()
assign_chips_for_labels(labels)

query = session.query(TruePositiveLabels)
query = query.join(species, species.id == TruePositiveLabels.species_id)
query = query.filter(species.name == 'false positive')
query = query.join(NOAAImage, TruePositiveLabels.image)
query = query.filter(NOAAImage.type == ImageType.RGB)
labels = query.all()
assign_chips_for_fps(labels)

# labels = session.query(Label).join(Hotspot, Hotspot.eo_label_id == Label.id)\
#     .join(NOAAImage, Label.image_id == NOAAImage.id).join(Label.species)\
#     .filter(NOAAImage.type == ImageType.RGB).all()
# assign_chips_for_labels(labels)

# Check for those not found
