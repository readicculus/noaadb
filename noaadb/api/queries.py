from noaadb.schema.models import IRImage, HeaderMeta, Flight, Camera, Survey, IRLabelEntry


def get_ir_with_sightings(s, survey=None, flight=None, cam=None):
    q = s.query(IRImage).join(IRLabelEntry, IRLabelEntry.image_id == IRImage.file_name)\
        .join(HeaderMeta)
    q = q.join(Camera)
    if cam: q = q.filter(Camera.cam_name==cam)
    q = q.join(Flight)
    if flight: q = q.filter(Flight.flight_name==flight)
    q = q.join(Survey)
    if survey: q = q.filter(Survey.name==survey)
    return q.all()

def get_ir_without_sightings(s, survey=None, flight=None, cam=None):
    q = s.query(IRImage).outerjoin(IRLabelEntry, IRLabelEntry.image_id == IRImage.file_name)\
        .filter(IRLabelEntry.image_id.is_(None))\
        .join(HeaderMeta)
    q = q.join(Camera)
    if cam: q = q.filter(Camera.cam_name == cam)
    q = q.join(Flight)
    if flight: q = q.filter(Flight.flight_name == flight)
    q = q.join(Survey)
    if survey: q = q.filter(Survey.name == survey)
    return q.all()
