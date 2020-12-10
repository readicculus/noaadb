import glob
import os

import luigi
import logging
###
# Ingest Kotz Images
###

from core import SQLAlchemyCustomTarget, ForcibleTask
from pipelines.ingest.tasks import CreateTableTask
from pipelines.ingest.util.image_size import get_image_size
from pipelines.ingest.util.image_utilities import  file_key, MetaParser, safe_int_cast, parse_kotz_filename, safe_float_cast, \
    flight_cam_id_from_dir
from noaadb import Session, DATABASE_URI
from noaadb.schema.models import *
from noaadb.schema.utils.queries import add_or_get_cam_flight_survey

KOTZ_MAPPINGS = {'CENT': 'C', 'LEFT': 'L', 'RIGHT': 'R', 'RGHT': 'R'}
class IngestKotzImageDirectoryTask(ForcibleTask):
    directory = luigi.Parameter()
    survey = luigi.Parameter()
    # dry_run = luigi.BoolParameter
    progress_interval = luigi.IntParameter(default=100)
    def requires(self):
        return CreateTableTask(children=["Survey", "Flight", "Camera", "HeaderMeta", "InstrumentMeta", "EOImage", "IRImage"])

    def output(self):
        return SQLAlchemyCustomTarget(DATABASE_URI, 'ingest_images', self.task_id, echo=False)

    def cleanup(self):
        logger = logging.getLogger('luigi-interface')
        flight_id, cam_id = flight_cam_id_from_dir(str(self.directory))

        s = Session()
        # get event_keys associated with this flight/cam
        cam_obj = s.query(Camera)\
            .filter(Camera.cam_name == cam_id)\
            .join(Flight).filter(Flight.flight_name == flight_id)\
            .join(Survey).filter(Survey.name == self.survey).first()
        if cam_obj is None:
            return
        # get unique keys
        ir_keys = s.query(IRImage.event_key).filter(IRImage.camera_id == cam_obj.id).all()
        eo_keys = s.query(EOImage.event_key).filter(EOImage.camera_id == cam_obj.id).all()
        logger.info('%d EO keys, %d IR keys to be removed' % (len(eo_keys), len(ir_keys)))
        unique_event_keys = set(ir_keys+eo_keys)
        # clear headers with those event keys
        headers_removed = s.query(HeaderMeta).filter(HeaderMeta.event_key.in_(unique_event_keys)).delete(synchronize_session=False)
        # clear ins with those event keys
        ins_removed = s.query(InstrumentMeta).filter(InstrumentMeta.event_key.in_(unique_event_keys)).delete(synchronize_session=False)
        # delete images with these keys
        ir_ims_removed = s.query(IRImage).filter(IRImage.event_key.in_(unique_event_keys)).delete(synchronize_session=False)
        eo_ims_removed = s.query(EOImage).filter(EOImage.event_key.in_(unique_event_keys)).delete(synchronize_session=False)
        s.commit()
        logger.info('Removed:'
                    '\n%d header objects'
                    '\n%d ins objects'
                    '\n%d EO image objects'
                    '\n%d IR image objects' % (headers_removed, ins_removed, eo_ims_removed, ir_ims_removed))
        s.close()
        self.output().remove()

    # Ingest All Images
    def run(self):
        logger = logging.getLogger('luigi-interface')
        # parse cam and flight id
        flight_id, cam_id = flight_cam_id_from_dir(str(self.directory))

        session = Session()
        # add cam to the database
        cam_obj = add_or_get_cam_flight_survey(session, cam_id, flight_id, survey=self.survey)
        session.commit()

        # get all
        eo_files = glob.glob(os.path.join(str(self.directory), '*_rgb.jpg'))
        ir_files = glob.glob(os.path.join(str(self.directory), '*_ir.tif'))
        meta_files = glob.glob(os.path.join(str(self.directory), '*_meta.json'))

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
                    meta = MetaParser(os.path.join(str(self.directory), fn))
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
                    w, h = get_image_size(os.path.join(str(self.directory), fn))
                im_obj = ImageC(
                    event_key=event_key,
                    filename=fn,
                    directory=self.directory,
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

