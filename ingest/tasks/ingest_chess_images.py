import glob
import logging
import os

import luigi

from ingest.tasks.create_tables import CreateTableTask
from ingest.util.image_size import get_image_size
from ingest.util.image_utilities import file_key, parse_chess_fn, safe_int_cast
from noaadb import Session, DATABASE_URI
from noaadb.schema.models import EOImage, IRImage
from noaadb.schema.utils.queries import add_or_get_cam_flight_survey
from luigi.contrib import sqla
# class TestClass(luigi.contrib.sqla.CopyToTable):
#
#     def rows(self):
#         yield IRImage()
#
#     def copy(self, conn, ins_rows, table_bound):

class IngestCHESSDirectoryTask(luigi.Task):
    image_dir = luigi.ListParameter()
    survey = luigi.Parameter()

    def requires(self): return CreateTableTask(children=["Survey", "Flight", "Camera", "HeaderMeta", "InstrumentMeta", "EOImage", "IRImage"])

    def output(self):
        return sqla.SQLAlchemyTarget(DATABASE_URI, 'ingest_images', self.task_id, echo=False)

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

class AggregateCHESSImagesTask(luigi.Task):
    image_directories = luigi.ListParameter()
    survey = luigi.Parameter()

    def requires(self):
        for image_dir in list(self.image_directories):
            yield IngestCHESSDirectoryTask(image_dir=image_dir, survey=self.survey)

    def output(self):
        return self.input()

    # Ingest All Images
    def run(self):


        print(self.image_directories)
        return 'A'

