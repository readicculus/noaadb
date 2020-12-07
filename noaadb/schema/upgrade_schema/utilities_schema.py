from noaadb import Session
from noaadb.schema.models import Camera
from noaadb.schema.models.archive.utilities import ImagePaths

source_engine = create_engine(DATABASE_URI)
# drop_utilities_schema()
# create_utilities_schema(source_engine, tables_only=False)

s = Session()

cams = s.query(Camera).all()
tomap = {
    'fl07_C': '/data3/imagery/kotz_2019/fl07/CENT/',
    'fl07_L': '/data3/imagery/kotz_2019/fl07/LEFT/',
    'fl06_C': '/data3/imagery/kotz_2019/fl06/CENT/',
    'fl06_L': '/data3/imagery/kotz_2019/fl06/LEFT/',
    'fl06_R': '/data3/imagery/kotz_2019/fl06/RIGHT/',
    'fl05_C': '/data3/imagery/kotz_2019/fl05/CENT/',
    'fl05_L': '/data3/imagery/kotz_2019/fl05/LEFT/',
    'fl05_R': '/data3/imagery/kotz_2019/fl05/RIGHT/',
    'fl04_C': '/data3/imagery/kotz_2019/fl05/CENT/',
    'fl04_L': '/data3/imagery/kotz_2019/fl05/LEFT/',
    'fl04_R': '/data3/imagery/kotz_2019/fl05/RIGHT/',
}
DEVICE='NAS'
for cam in cams:
    if cam.flight.survey.name == 'test_kotz_2019':
        k = '%s_%s' % (cam.flight.flight_name, cam.cam_name)
        if k in tomap:
            obj = ImagePaths(cam_id=cam.id, path=tomap[k], device='NAS')
            s.add(obj)
s.commit()
s.close()