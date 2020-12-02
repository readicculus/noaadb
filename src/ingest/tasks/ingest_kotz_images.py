import glob
import os

import luigi
import logging
###
# Ingest Kotz Images
###
from luigi.contrib import sqla

from ingest.tasks import CreateTableTask
from ingest.util import get_image_size
from ingest.util import file_key, MetaParser, safe_int_cast, parse_kotz_filename, safe_float_cast
from noaadb import Session, DATABASE_URI
from noaadb.schema.models import HeaderMeta, EOImage, IRImage, InstrumentMeta
from noaadb.schema.utils.queries import add_or_get_cam_flight_survey

KOTZ_MAPPINGS = {'CENT': 'C', 'LEFT': 'L', 'RIGHT': 'R', 'RGHT': 'R'}
class IngestKotzDirectoryTask(luigi.Task):
    device_dir = luigi.Parameter()
    survey = luigi.Parameter()
    # dry_run = luigi.BoolParameter
    progress_interval = luigi.IntParameter(default=100)
    def requires(self):
        return CreateTableTask(children=["Survey", "Flight", "Camera", "HeaderMeta", "InstrumentMeta", "EOImage", "IRImage"])

    def output(self):
        return sqla.SQLAlchemyTarget(DATABASE_URI, 'ingest_images', self.task_id, echo=False)

    # Ingest All Images
    def run(self):
        logger = logging.getLogger('luigi-interface')
        # parse cam and flight id
        p, c = os.path.split(str(self.device_dir))
        flight_id = os.path.split(p)[1]
        cam_id = KOTZ_MAPPINGS[c]

        session = Session()
        # add cam to the database
        cam_obj = add_or_get_cam_flight_survey(session, cam_id, flight_id, survey=self.survey)
        session.commit()

        # get all
        eo_files = glob.glob(os.path.join(str(self.device_dir), '*_rgb.jpg'))
        ir_files = glob.glob(os.path.join(str(self.device_dir), '*_ir.tif'))
        meta_files = glob.glob(os.path.join(str(self.device_dir), '*_meta.json'))

        # Group files by key for all  _ir, _rgb, _meta files.
        grouped_files = {}
        for file in eo_files + ir_files + meta_files:
            fn = os.path.basename(file)
            k = file_key(fn)
            if not k in grouped_files:
                grouped_files[k] = []
            grouped_files[k].append(fn)

        # Iterate groups and handle each respective file type
        total = len(grouped_files)
        self.set_tracking_url("stdout")
        for i, event_key in enumerate(grouped_files):
            if i % int(self.progress_interval) == 0:
                self.set_status_message("Progress: %d / %d" % (i,total))
                # displays a progress bar in the scheduler UI
                self.set_progress_percentage(i/total * 100)
                logger.info("Progress: %d / %d" % (i, total))

            flight, cam, timestamp = parse_kotz_filename(event_key)
            files = grouped_files[event_key]
            meta = None
            for fn in files:
                if fn.endswith('_meta.json'):
                    meta = MetaParser(os.path.join(str(self.device_dir), fn))
                    break

            # Add the header object
            header_exists = session.query(HeaderMeta).filter(
                InstrumentMeta.event_key == event_key).first() is not None
            if meta and not header_exists:
                header = meta.get_header()
                header_obj = HeaderMeta(
                    event_key=event_key,
                    stamp=safe_int_cast(header.get('stamp')),
                    frame_id=header.get('frame_id'),
                    seq=safe_int_cast(header.get('seq')),
                    camera=cam_obj
                )
                session.add(header_obj)

            # Add the INS object
            ins_exists = session.query(InstrumentMeta).filter(
                InstrumentMeta.event_key == event_key).first() is not None
            if meta and not ins_exists:
                ins_meta = meta.get_ins()
                ins_obj = InstrumentMeta(
                    event_key=event_key,
                    track_angle=safe_float_cast(ins_meta.get("track_angle")),
                    angular_rate_x=safe_float_cast(ins_meta.get("angular_rate_x")),
                    angular_rate_y=safe_float_cast(ins_meta.get("angular_rate_y")),
                    angular_rate_z=safe_float_cast(ins_meta.get("angular_rate_z")),
                    down_velocity=safe_float_cast(ins_meta.get("down_velocity")),
                    pitch=safe_float_cast(ins_meta.get("pitch")),
                    altitude=safe_float_cast(ins_meta.get("altitude")),
                    north_velocity=safe_float_cast(ins_meta.get("north_velocity")),
                    acceleration_y=safe_float_cast(ins_meta.get("acceleration_y")),
                    gnss_status=safe_int_cast(ins_meta.get("gnss_status")),
                    longitude=safe_float_cast(ins_meta.get("longitude")),
                    roll=safe_float_cast(ins_meta.get("roll")),
                    acceleration_x=safe_float_cast(ins_meta.get("acceleration_x")),
                    align_status=safe_int_cast(ins_meta.get("align_status")),
                    total_speed=safe_float_cast(ins_meta.get("total_speed")),
                    time=safe_float_cast(ins_meta.get("time")),
                    latitude=safe_float_cast(ins_meta.get("latitude")),
                    heading=safe_float_cast(ins_meta.get("heading")),
                    east_velocity=safe_float_cast(ins_meta.get("east_velocity")),
                    acceleration_z=safe_float_cast(ins_meta.get("acceleration_z"))
                )
                session.add(ins_obj)

            # Add the image objects
            for fn in files:
                im_type = 'ir' if '_ir.' in fn else 'rgb' if '_rgb.' in fn else None
                # skip if not _ir or _rgb image file
                if not im_type:
                    continue
                ImageC = EOImage if im_type == 'rgb' else IRImage
                # skip if image already in database
                im_exists = session.query(ImageC).filter(ImageC.filename == fn).first() is not None
                if im_exists:
                    continue

                im_meta = meta.get_image(im_type) if meta else {}
                c = 3 if im_type == 'rgb' else 1
                w, h = safe_int_cast(im_meta.get("width")), safe_int_cast(im_meta.get("height"))
                if w is None or h is None:
                    w, h = get_image_size(os.path.join(str(self.device_dir), fn))
                im_obj = ImageC(
                    event_key=event_key,
                    filename=fn,
                    directory=self.device_dir,
                    width=w,
                    height=h,
                    depth=c,
                    timestamp=timestamp,
                    is_bigendian=safe_int_cast(im_meta.get("is_bigendian")),
                    step=safe_int_cast(im_meta.get("step")),
                    encoding=im_meta.get('encoding'),
                    camera=cam_obj
                )
                session.add(im_obj)

        session.commit()
        session.close()
        self.output().touch()

class AggregateKotzImagesTask(luigi.Task):
    image_directories = luigi.ListParameter()
    survey = luigi.Parameter()
    def requires(self):
        # for device_dir in list(self.image_directories):
        #     yield IngestKotzDirectoryTask(device_dir=device_dir, survey=self.survey)
        #
        return [IngestKotzDirectoryTask(device_dir=device_dir, survey=self.survey) for device_dir in list(self.image_directories)]

    def output(self):
        return self.input()

# class IngestKotzImages(luigi.Task):
#     def requires(self):
#         parent_dir = "/data2/2019"
#         subdirectories = ["fl01/CENT", "fl01/LEFT",
#                           "fl04/CENT", "fl04/LEFT",
#                           "fl05/CENT", "fl05/LEFT",
#                           "fl06/CENT", "fl06/LEFT",
#                           "fl07/CENT", "fl07/LEFT"]
#         subdirectories = subdirectories[:2]
#         image_directories = [os.path.join(parent_dir, sd) for sd in subdirectories]
#         for im_dir in image_directories:
#             yield IngestSurveyImagesTask(im_dir)
#
#     #
#     def output(self):
#         return None
#
#     # Ingest All Images
#     def run(self):
#         return None # print('ABC %s' % self.input())

