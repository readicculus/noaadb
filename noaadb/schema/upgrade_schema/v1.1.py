from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy import DDL
from noaadb import DATABASE_URI
from noaadb.schema.models import Label, NOAAImage, ImageType, Hotspot, LabelChips, Chip, ImageDimension

from scripts.util import printProgressBar



engine = create_engine(DATABASE_URI, echo=False)

engine.execute(DDL("CREATE SCHEMA IF NOT EXISTS chips"))


LabelChips.__table__.drop(bind=engine, checkfirst=True)
Chip.__table__.drop(bind=engine, checkfirst=True)
ImageDimension.__table__.drop(bind=engine, checkfirst=True)
ImageDimension.__table__.create(bind=engine)
Chip.__table__.create(bind=engine)
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

chip_w, chip_h, chip_overlap = 640,640, 120
cw,ch,co = [416,640,832,1248], [416, 640,832,1248], [120, 120,120,120]
for chip_w, chip_h, chip_overlap in zip(cw,ch,co):
    for k in image_dims:
        dims = image_dims[k]
        tiles = tile_image(dims.width,dims.height, chip_w, chip_h, chip_overlap)

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
# Populate ChipHotspot Table
labels = session.query(Label).join(Hotspot, Hotspot.eo_label_id == Label.id)\
    .join(NOAAImage, Label.image_id == NOAAImage.id).join(Label.species)\
    .filter(NOAAImage.type == ImageType.RGB).all()

def percent_on(chip, label):

    dx = min(chip.x2, label.x2) - max(chip.x1, label.x1)
    dy = min(chip.y2, label.y2) - max(chip.y1, label.y1)
    if (dx < 0) and (dy < 0):
        return None
    intersected_area = dx * dy
    label_area = (label.x2 - label.x1) * (label.y2 - label.y1)
    return intersected_area / label_area

for i,label in enumerate(labels):
    printProgressBar(i, len(labels), prefix='Progress:', suffix='Complete', length=50, printEnd = "")
    im_w = label.image.width
    im_h = label.image.height
    key = "%dx%d" % (im_w, im_h)
    im_dim = image_dims[key]
    # chips_containing_label = session.query(Chip).filter(Chip.image_dimension_id == im_dim.id)\
    #     .filter(and_(Chip.x1 <= label.x1,Chip.x2 >= label.x2, Chip.y1 <= label.y1,Chip.y2 >= label.y2))\
    #     .all()
    chips_containing_partial__label = session.query(Chip).filter(Chip.image_dimension_id == im_dim.id) \
        .filter(and_(Chip.x1 <= label.x2, label.x1 <= Chip.x2,Chip.y1 <= label.y2, label.y1 <= Chip.y2)) \
        .all()

    for chip in chips_containing_partial__label:
        ch = LabelChips(label=label, chip=chip, percent_intersection=percent_on(chip,label))
        session.add(ch)
    if i % 500 == 0:
        session.commit()
session.commit()


# Check for those not found
