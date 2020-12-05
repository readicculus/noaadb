import glob
import logging
import os

import luigi

from core import ForcibleTask, SQLAlchemyCustomTarget
from ingest.tasks import CreateTableTask
from ingest.util import get_image_size
from ingest.util import file_key, parse_chess_fn
from noaadb import Session, DATABASE_URI
from noaadb.schema.models import *
from noaadb.schema.utils.queries import add_or_get_cam_flight_survey

class IngestCHESSDirectoryTask(ForcibleTask):
    image_dir = luigi.ListParameter()
    survey = luigi.Parameter()

    def requires(self):
        return CreateTableTask(children=["Survey", "Flight", "Camera", "HeaderMeta", "InstrumentMeta", "EOImage", "IRImage"])

    def output(self):
        return SQLAlchemyCustomTarget(DATABASE_URI, 'ingest_images', os.path.basename(self.image_dir), echo=False)

    def cleanup(self):
        logger = logging.getLogger('luigi-interface')

        s = Session()
        # get event_keys associated with this flight/cam
        cam_ids = s.query(Camera.id)\
            .join(Flight)\
            .join(Survey).filter(Survey.name == self.survey).all()
        if len(cam_ids) == 0:
            return
            # get unique keys
        ir_keys = s.query(IRImage.event_key).filter(IRImage.camera_id.in_(cam_ids)).all()
        eo_keys = s.query(EOImage.event_key).filter(EOImage.camera_id.in_(cam_ids)).all()
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

    def run(self):
        logger = logging.getLogger('luigi-interface')
        files = []
        # eo_files = glob.glob(os.path.join(image_dir, '*_rgb*'))
        # ir_files = glob.glob(os.path.join(image_dir, '*_ir*'))
        eo_files = glob.glob(os.path.join(str(self.image_dir), '*_COLOR-8*'))
        ir_files = glob.glob(os.path.join(str(self.image_dir), '*_THERM-16*'))
        files += eo_files + ir_files

        # Group files by key for all  _ir, _rgb files.
        grouped_files = {}
        for file in files:
            fn = os.path.basename(file)
            k = file_key(fn)
            if not k in grouped_files:
                grouped_files[k] = []
            grouped_files[k].append(fn)

        total = len(grouped_files)
        self.set_tracking_url("stdout")
        session = Session()
        for i, event_key in enumerate(grouped_files):
            if i % 100 == 0:
                self.set_status_message("Progress: %d / %d" % (i, total))
                # displays a progress bar in the scheduler UI
                self.set_progress_percentage(i / total * 100)
                logger.info("Progress: %d / %d" % (i, total))

            flight_id, cam_id, timestamp = parse_chess_fn(event_key)
            # add cam to the database
            cam_obj = add_or_get_cam_flight_survey(session, cam_id, flight_id, survey=self.survey)

            files = grouped_files[event_key]
            for fn in files:
                im_type = 'ir' if 'THERM-16' in fn else 'rgb' if 'COLOR-8' in fn else None
                assert (im_type is not None)

                ImageC = EOImage if im_type == 'rgb' else IRImage
                # skip if image already in database
                im_exists = session.query(ImageC).filter(ImageC.event_key == event_key).first() is not None
                if im_exists:
                    continue

                c = 3 if im_type == 'rgb' else 1
                w, h = get_image_size(os.path.join(str(self.image_dir), fn))
                im_obj = ImageC(
                    event_key=event_key,
                    filename=fn,
                    directory=self.image_dir,
                    width=w,
                    height=h,
                    depth=c,
                    timestamp=timestamp,
                    is_bigendian=None,
                    step=None,
                    encoding=None,
                    camera=cam_obj
                )
                session.add(im_obj)

        session.commit()
        self.output().touch()
        session.close()

class IngestCHESSImagesTask(ForcibleTask):
    image_directories = luigi.ListParameter()
    survey = luigi.Parameter()

    def requires(self):
        return CreateTableTask(children=["Survey", "Flight", "Camera", "HeaderMeta", "InstrumentMeta", "EOImage", "IRImage"])

    def output(self):
        return SQLAlchemyCustomTarget(DATABASE_URI, 'ingest_images', self.task_id, echo=False)

    def cleanup(self):
        logger = logging.getLogger('luigi-interface')

        s = Session()
        # get event_keys associated with this flight/cam
        cam_ids = s.query(Camera.id)\
            .join(Flight)\
            .join(Survey).filter(Survey.name == self.survey).all()
        if len(cam_ids) == 0:
            return
            # get unique keys
        ir_keys = s.query(IRImage.event_key).filter(IRImage.camera_id.in_(cam_ids)).all()
        eo_keys = s.query(EOImage.event_key).filter(EOImage.camera_id.in_(cam_ids)).all()
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

    def run(self):
        logger = logging.getLogger('luigi-interface')
        for image_dir in self.image_directories:
            files = []
            # eo_files = glob.glob(os.path.join(image_dir, '*_rgb*'))
            # ir_files = glob.glob(os.path.join(image_dir, '*_ir*'))
            eo_files = glob.glob(os.path.join(str(image_dir), '*_COLOR-8*'))
            ir_files = glob.glob(os.path.join(str(image_dir), '*_THERM-16*'))
            files += eo_files + ir_files

            # Group files by key for all  _ir, _rgb files.
            grouped_files = {}
            for file in files:
                fn = os.path.basename(file)
                k = file_key(fn)
                if not k in grouped_files:
                    grouped_files[k] = []
                grouped_files[k].append(fn)

            total = len(grouped_files)
            self.set_tracking_url("stdout")
            session = Session()
            for i, event_key in enumerate(grouped_files):
                if i % 100 == 0:
                    self.set_status_message("Progress: %d / %d" % (i, total))
                    # displays a progress bar in the scheduler UI
                    self.set_progress_percentage(i / total * 100)
                    logger.info("Progress: %d / %d" % (i, total))

                flight_id, cam_id, timestamp = parse_chess_fn(event_key)
                # add cam to the database
                cam_obj = add_or_get_cam_flight_survey(session, cam_id, flight_id, survey=self.survey)

                files = grouped_files[event_key]
                for fn in files:
                    im_type = 'ir' if 'THERM-16' in fn else 'rgb' if 'COLOR-8' in fn else None
                    assert (im_type is not None)

                    ImageC = EOImage if im_type == 'rgb' else IRImage
                    # skip if image already in database
                    im_exists = session.query(ImageC).filter(ImageC.event_key == event_key).first() is not None
                    if im_exists:
                        continue

                    c = 3 if im_type == 'rgb' else 1
                    w, h = get_image_size(os.path.join(str(image_dir), fn))
                    im_obj = ImageC(
                        event_key=event_key,
                        filename=fn,
                        directory=image_dir,
                        width=w,
                        height=h,
                        depth=c,
                        timestamp=timestamp,
                        is_bigendian=None,
                        step=None,
                        encoding=None,
                        camera=cam_obj
                    )
                    session.add(im_obj)

            session.commit()
            self.output().touch()
            session.close()


