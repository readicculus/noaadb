import glob
import logging
import os

import luigi
import collections
import logging
import os

import luigi.contrib.hdfs
import sqlalchemy
from luigi.contrib.sqla import SQLAlchemyTarget
from sqlalchemy import DDL
from core import ForcibleTask, SQLAlchemyCustomTarget
from noaadb import Session, DATABASE_URI
from noaadb.schema.models.survey_data import EOImage, IRImage
from noaadb.schema.utils.queries import add_or_get_cam_flight_survey
from datetime import datetime

from pipelines.ingest.util.image_size import get_image_size

### ==================== CHESS_us Images ====================
from pipelines.ingest.util.image_utilities import parse_chess_fn, file_key, parse_beaufort_filename


class Ingest_pb_us_ImagesTask(ForcibleTask):
    image_directory = luigi.Parameter()
    def require(self):
        return CreateTableTask(
            children=["Survey", "Flight", "Camera", "HeaderMeta", "InstrumentMeta", "EOImage", "IRImage"])

    def output(self):
        return SQLAlchemyCustomTarget(DATABASE_URI, 'ingest_images', self.task_id, echo=False)


    def cleanup(self):
        pass

    def run(self):
        logger = logging.getLogger('luigi-interface')
        eo_files = glob.glob(os.path.join(str(self.image_directory), '*COLOR-8-BIT.JPG'))
        s = Session()
        images = []
        num_existing = 0
        for im_file in eo_files:
            fn = os.path.basename(im_file)
            image_key = file_key(fn)
            check = s.query(EOImage).filter(EOImage.event_key == image_key).first()
            if check is not None:
                num_existing += 1
                continue
            flight, cam, timestamp = parse_chess_fn(fn)
            cam_obj = add_or_get_cam_flight_survey(s, cam, flight, survey='CHESS_2016')
            w, h = get_image_size(im_file)
            im_obj = EOImage(
                event_key=image_key,
                filename=fn,
                directory=self.image_directory,
                width=w,
                height=h,
                depth=3,
                timestamp=timestamp,
                camera=cam_obj
            )
            images.append(im_obj)
            s.add(im_obj)
            s.flush()
        s.commit()
        self.output().touch()
        s.close()
        logger.info("Added %d images. %d already existed." % (len(images), num_existing))

### ==================== Beaufort 19 Images ====================
class Ingest_pb_beufort_19_ImagesTask(ForcibleTask):
    image_directory = luigi.Parameter()
    def require(self):
        return CreateTableTask(
            children=["Survey", "Flight", "Camera", "HeaderMeta", "InstrumentMeta", "EOImage", "IRImage"])

    def output(self):
        return SQLAlchemyCustomTarget(DATABASE_URI, 'ingest_images', self.task_id, echo=False)

    def cleanup(self):
        pass

    def run(self):
        logger = logging.getLogger('luigi-interface')
        eo_files = glob.glob(os.path.join(str(self.image_directory), '*_rgb.jpg'))
        ir_files = glob.glob(os.path.join(str(self.image_directory), '*_ir.tif'))

        survey = '2019_beaufort'
        s = Session()
        added = []
        eo_ct, ir_ct = 0, 0
        for f in eo_files+ir_files:
            fn = os.path.basename(f)
            is_rgb = 'rgb' in fn
            ImageObj = EOImage if is_rgb else IRImage
            if is_rgb: eo_ct += 1
            else: ir_ct += 1
            key = file_key(fn)[4:]
            flight, cam, timestamp = parse_beaufort_filename(key)
            w, h = get_image_size(f)
            cam_obj = add_or_get_cam_flight_survey(s, cam, flight, survey)
            im_obj = ImageObj(
                event_key=key,
                filename=fn,
                directory=self.image_directory,
                width=w,
                height=h,
                depth=3,
                timestamp=timestamp,
                camera=cam_obj
            )
            s.add(im_obj)
            s.flush()
            added.append(im_obj)
        logger.info('%d EO Images added' % eo_ct)
        logger.info('%d IR Images added' % ir_ct)
        s.commit()
        s.close()
        self.output().touch()

### ==================== CHESS_ru Images ====================
class Ingest_pb_ru_ImagesTask(ForcibleTask):
    image_directory = luigi.Parameter()
    def require(self):
        return CreateTableTask(
            children=["Survey", "Flight", "Camera", "HeaderMeta", "InstrumentMeta", "EOImage", "IRImage"])

    def output(self):
        return SQLAlchemyCustomTarget(DATABASE_URI, 'ingest_images', self.task_id, echo=False)

    def _get_cam(self, s):
        return add_or_get_cam_flight_survey(s, 'X', 'X', survey='CHESS_ru')

    def cleanup(self):
        logger = logging.getLogger('luigi-interface')

        s = Session()
        cam_obj = self._get_cam(s)

        if cam_obj is None:
            return
        # get unique keys
        ir_keys = s.query(IRImage.event_key).filter(IRImage.camera_id == cam_obj.id).all()
        eo_keys = s.query(EOImage.event_key).filter(EOImage.camera_id == cam_obj.id).all()
        logger.info('%d EO keys, %d IR keys to be removed' % (len(eo_keys), len(ir_keys)))
        unique_event_keys = set(ir_keys+eo_keys)

        ir_ims_removed = s.query(IRImage).filter(IRImage.event_key.in_(unique_event_keys)).delete(synchronize_session=False)
        eo_ims_removed = s.query(EOImage).filter(EOImage.event_key.in_(unique_event_keys)).delete(synchronize_session=False)
        s.commit()
        logger.info('Removed:'
                    '\n%d EO image objects'
                    '\n%d IR image objects' % (eo_ims_removed, ir_ims_removed))
        s.close()
        self.output().remove()

    def run(self):
        s = Session()
        cam_obj = self._get_cam(s)
        eo_files = glob.glob(os.path.join(str(self.image_directory), '*.JPG'))
        for file in eo_files:
            fn = os.path.basename(file)
            file_key = fn.replace('.JPG', '')
            time_str = file_key.split('_')[2]
            ts = datetime.strptime(time_str, "%Y-%m-%d %H-%M-%S").timestamp()
            timestamp = datetime.fromtimestamp(ts)
            w, h = get_image_size(file)

            im_obj = EOImage(
                event_key=file_key,
                filename=fn,
                directory=self.image_directory,
                width=w,
                height=h,
                depth=3,
                timestamp=timestamp,
                camera=cam_obj
            )
            s.add(im_obj)
            s.flush()

        s.commit()
        s.close()
        self.output().touch()
