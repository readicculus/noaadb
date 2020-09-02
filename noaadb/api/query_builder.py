from sqlalchemy.orm import Query

from noaadb.schema.models import *
def qb_image_without_sightings(type, surveys=[], flights=[], cams=[]):
    ImageCls = EOImage if type == 'eo' else IRImage if type == 'ir' else None
    LabelCls = EOLabelEntry if type == 'eo' else IRLabelEntry if type == 'ir' else None

    q = Query(ImageCls).outerjoin(LabelCls, LabelCls.image_id == ImageCls.file_name) \
        .filter(LabelCls.image_id.is_(None))
    q = q.join(HeaderMeta)
    q = q.join(Camera)
    if len(cams) > 0: q = q.filter(Camera.cam_name.in_(cams))
    q = q.join(Flight)
    if len(flights) > 0: q = q.filter(Flight.flight_name.in_(flights))
    q = q.join(Survey)
    if len(surveys) > 0: q = q.filter(Survey.name.in_(surveys))
    q = q.distinct(ImageCls.file_name)
    return q



def qb_image_with_sightings(type, surveys=[], flights=[], cams=[], species=[]):
    ImageCls = EOImage if type == 'eo' else IRImage if type == 'ir' else None
    LabelCls = EOLabelEntry if type == 'eo' else IRLabelEntry if type == 'ir' else None

    q = Query(ImageCls).join(LabelCls, LabelCls.image_id == ImageCls.file_name) \
        .join(Species, LabelCls.species_id == Species.id)
    if len(species) > 0:
        q = q.filter(Species.name.in_(species))
    q = q.join(HeaderMeta)
    q = q.join(Camera)
    if len(cams) > 0: q = q.filter(Camera.cam_name.in_(cams))
    q = q.join(Flight)
    if len(flights) > 0: q = q.filter(Flight.flight_name.in_(flights))
    q = q.join(Survey)
    if len(surveys) > 0: q = q.filter(Survey.name.in_(surveys))
    q = q.distinct(ImageCls.file_name)
    return q

def qb_images(type, surveys=[], flights=[], cams=[]):
    ImageCls = EOImage if type == 'eo' else IRImage if type == 'ir' else None

    q = Query(ImageCls)
    q = q.join(HeaderMeta)
    q = q.join(Camera)
    if len(cams) > 0: q = q.filter(Camera.cam_name.in_(cams))
    q = q.join(Flight)
    if len(flights) > 0: q = q.filter(Flight.flight_name.in_(flights))
    q = q.join(Survey)
    if len(surveys) > 0: q = q.filter(Survey.name.in_(surveys))
    return q

def qb_labels(type, surveys=[], flights=[], cams=[], species=[]):
    ImageCls = EOImage if type == 'eo' else IRImage if type == 'ir' else None
    LabelCls = EOLabelEntry if type == 'eo' else IRLabelEntry if type == 'ir' else None

    q = Query(LabelCls).join(ImageCls, LabelCls.image_id == ImageCls.file_name) \
        .join(Species, LabelCls.species_id == Species.id)
    if len(species) > 0:
        q = q.filter(Species.name.in_(species))
    q = q.join(HeaderMeta)
    q = q.join(Camera)
    if len(cams) > 0: q = q.filter(Camera.cam_name.in_(cams))
    q = q.join(Flight)
    if len(flights) > 0: q = q.filter(Flight.flight_name.in_(flights))
    q = q.join(Survey)
    if len(surveys) > 0: q = q.filter(Survey.name.in_(surveys))
    return q

# def qb_eo_image(s, surveys=None, flights=None, cams=None):
#     q = s.query(EOImage) \
#         .join(HeaderMeta)
#     q = q.join(Camera)
#     if len(cams) > 0: q = q.filter(Camera.cam_name.in_(cams))
#     q = q.join(Flight)
#     if len(flights) > 0: q = q.filter(Flight.flight_name.in_(flights))
#     q = q.join(Survey)
#     if len(surveys) > 0: q = q.filter(Survey.name.in_(surveys))
#     return q
