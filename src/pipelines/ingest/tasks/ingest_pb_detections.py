import glob
import logging
import os

import luigi
from sqlalchemy import or_

from core import ForcibleTask, SQLAlchemyCustomTarget
from noaadb import DATABASE_URI, Session
from noaadb.schema.models.survey_data import *
from noaadb.schema.models.annotation_data import *
from noaadb.schema.utils.queries import add_species_if_not_exist, add_job_if_not_exists, add_worker_if_not_exists
from pipelines.ingest.tasks import Ingest_pb_ru_ImagesTask, Ingest_pb_us_ImagesTask, Ingest_pb_beufort_19_ImagesTask
from pipelines.ingest.util.image_utilities import file_key
from pipelines.ingest.util.parse_VOC import parse_VOC_xml
import pandas as pd


### ==================== CHESS_us Detections ====================
class Ingest_pb_us_DetectionsTask(ForcibleTask):
    annotations_csv = luigi.Parameter()

    def require(self):
        return Ingest_pb_us_ImagesTask()

    def output(self):
        return SQLAlchemyCustomTarget(DATABASE_URI, 'LoadDetectionTask', self.task_id, echo=False)

    def cleanup(self):
        pass

    def _check_missing(self, df):
        s = Session()
        checked = []
        missing = []
        for i, row in df.iterrows():
            if pd.isna(row['Frame']):
                # last real row is blank
                break
            im_fn = row['Frame']

            im_fn = im_fn[4:]
            is_color = 'COLOR' in im_fn
            key = file_key(im_fn)
            if im_fn in checked:
                continue
            checked.append(im_fn)
            ImageObj = EOImage if is_color else IRImage
            im = s.query(ImageObj).filter(ImageObj.event_key == key).first()
            exists = im is not None
            print("%s exists: %s" % (im_fn, exists))
            if exists:
                annots = s.query(Annotation, Species) \
                    .filter(or_(Annotation.eo_event_key == key, Annotation.ir_event_key == key)) \
                    .join(Species).filter(Species.name == 'Polar Bear').all()
                print("%d Polar Bears already in %s" % (len(annots), im_fn))
            else:
                missing.append(im_fn)
        print("Missing:")
        for m in missing:
            print(m)
        s.close()
        return missing

    def run(self):
        df = pd.read_csv(self.annotations_csv, header=[0])
        missing = self._check_missing(df)
        # if len(missing) > 0:
        # #     return
        # for i, row in df.iterrows():
        #     if pd.isna(row['Frame']):
        #         # last real row is blank
        #         break
        #     im_fn = row['Frame']
        #
        #     im_fn = im_fn[4:]
        #     is_color = 'COLOR' in im_fn
        #     key = file_key(im_fn)
        #     if im_fn in checked:
        #         continue
        #     checked.append(im_fn)
        #     ImageObj = EOImage if is_color else IRImage
        #     im = s.query(ImageObj).filter(ImageObj.event_key == key).first()
        #     exists = im is not None
        #     print("%s exists: %s" % (im_fn, exists))
        #     if exists:
        #         annots = s.query(Annotation, Species) \
        #             .filter(or_(Annotation.eo_event_key == key, Annotation.ir_event_key == key)) \
        #             .join(Species).filter(Species.name == 'Polar Bear').all()
        #         print("%d Polar Bears already in %s" % (len(annots), im_fn))
        #     else:
        #         missing.append(im_fn)
        # print("Missing:")
        # for m in missing:
        #     print(m)
        # s.close()
        # return missing

### ==================== Beaufort 19 Detections ====================
class Ingest_pb_beufort_19_DetectionsTask(ForcibleTask):
    xml_directory = luigi.Parameter()
    def requires(self):
        return Ingest_pb_beufort_19_ImagesTask()

    def output(self):
        return SQLAlchemyCustomTarget(DATABASE_URI, 'LoadDetectionTask', self.task_id, echo=False)

    def cleanup(self):
        s=Session()
        eo_keys = s.query(EOImage.event_key) \
            .join(Camera, EOImage.camera).join(Flight, Camera.flight).join(Survey, Flight.survey).filter(
            Survey.name == '2019_beaufort').all()
        eo_keys = [k for (k,) in eo_keys]
        ir_keys = s.query(IRImage.event_key) \
            .join(Camera, IRImage.camera).join(Flight, Camera.flight).join(Survey, Flight.survey).filter(
            Survey.name == '2019_beaufort').all()
        ir_keys = [k for (k,) in ir_keys]
        for a in s.query(Annotation).filter(Annotation.ir_event_key.in_(ir_keys)).all():
            if a.eo_box: s.delete(a.eo_box)
            if a.ir_box: s.delete(a.ir_box)
            s.delete(a)
        s.flush()
        for a in s.query(Annotation).filter(Annotation.eo_event_key.in_(eo_keys)).all():
            if a.eo_box: s.delete(a.eo_box)
            if a.ir_box: s.delete(a.ir_box)
            s.delete(a)
        s.flush()
        s.commit()
        s.close()
        self.output().remove()

    def _make_im_dict(self):
        ir_images = glob.glob(os.path.join(str(self.xml_directory), '*ir.tif'))
        eo_images = glob.glob(os.path.join(str(self.xml_directory), '*rgb.tif'))
        im_dict = {}
        for x in ir_images + eo_images:
            fn = os.path.basename(x)
            key = fn[:3]
            if not key in im_dict:
                im_dict[key] = {'ir': {'boxes': []}, 'eo': {'boxes': []}}
            is_eo = 'rgb' in fn
            im_dict[key]['eo' if is_eo else 'ir']['fn'] = fn

        # load detections
        xml_files = glob.glob(os.path.join(str(self.xml_directory), '*.xml'))
        for file in xml_files:
            name, boxes = parse_VOC_xml(file)

            name = name.replace('_ir_8bit.jpg', '_ir.tif')
            is_eo = 'rgb' in name
            key = os.path.basename(name)[:3]
            for box in boxes:
                if box[0][-1] != 's':
                    im_dict[key]['eo' if is_eo else 'ir']['boxes'].append(box)
        return im_dict

    def run(self):
        logger = logging.getLogger('luigi-interface')
        im_dict = self._make_im_dict()
        s = Session()
        eo_job = add_job_if_not_exists(s, 'Original_Beafort_19_job', '')
        eo_worker = add_worker_if_not_exists(s, 'Original_Beafort_19_annotator', True)

        yuval_job = add_job_if_not_exists(s, 'add_ir_pb_labels_job',
                                          'Yuval went through and added IR labels as we only had color labels for 2019 beafort polar bears.')
        yuval_worker = add_worker_if_not_exists(s, 'Yuval', True)
        species = add_species_if_not_exist(s, 'Polar Bear')
        new_annotations = []



        for k in im_dict:
            eo_key = file_key(im_dict[k]['eo']['fn'])[4:]
            ir_key = None if 'fn' not in im_dict[k]['ir'] else file_key(im_dict[k]['ir']['fn'])[4:]
            eo = im_dict[k]['eo']['boxes']
            ir = im_dict[k]['ir']['boxes']
            if len(eo) == len(ir):
                # add as Annotation pairs
                sorted(eo, key=lambda x: "%d%d" % (x[1], x[2])) # sort by x1y1 top,left corner
                sorted(ir, key=lambda x: "%d%d" % (x[1], x[2])) # sort by x1y1 top,left corner
                for eo_box, ir_box in zip(eo,ir):
                    ir_box[0] = eo_box[0]
                for eo_box, ir_box in zip(eo,ir):
                    db_eo_box = BoundingBox(x1=eo_box[1],
                                            y1=eo_box[2],
                                            x2=eo_box[3],
                                            y2=eo_box[4],
                                            worker=eo_worker,
                                            job=eo_job)
                    db_ir_box = BoundingBox(x1=ir_box[1],
                                            y1=ir_box[2],
                                            x2=ir_box[3],
                                            y2=ir_box[4],
                                            worker=yuval_worker,
                                            job=yuval_job)
                    s.add(db_eo_box)
                    s.add(db_ir_box)
                    s.flush()
                    annot = Annotation(eo_event_key=eo_key,
                                       ir_event_key=ir_key,
                                       ir_box=db_eo_box,
                                       eo_box=db_ir_box,
                                       species=species,
                                       is_shadow=False,
                                       hotspot_id=eo_box[0]
                                       )
                    s.add(annot)
                    s.flush()
                    new_annotations.append(annot)
            else:
                # add as independent Annotations
                for eo_box in eo:
                    db_eo_box = BoundingBox(x1=eo_box[1],
                                            y1=eo_box[2],
                                            x2=eo_box[3],
                                            y2=eo_box[4],
                                            worker=eo_worker,
                                            job=eo_job)
                    s.add(db_eo_box)
                    s.flush()
                    annot = Annotation(eo_event_key=eo_key,
                                       eo_box=db_eo_box,
                                       species=species,
                                       is_shadow=False,
                                       hotspot_id=eo_box[0]
                                       )
                    s.add(annot)
                    s.flush()
                    new_annotations.append(annot)

                for ir_box in ir:
                    db_ir_box = BoundingBox(x1=ir_box[1],
                                            y1=ir_box[2],
                                            x2=ir_box[3],
                                            y2=ir_box[4],
                                            worker=yuval_worker,
                                            job=yuval_job)
                    s.add(db_ir_box)
                    s.flush()
                    annot = Annotation(ir_event_key=ir_key,
                                       ir_box=db_ir_box,
                                       species=species,
                                       is_shadow=False
                                       )
                    s.add(annot)
                    s.flush()
                    new_annotations.append(annot)
        logger.info("%d Annotations added" % len(new_annotations))
        ct_ir_only = 0
        ct_eo_only = 0
        ct_both = 0
        for a in new_annotations:
            if a.ir_box is not None and a.eo_box is not None:
                ct_both+=1
            elif a.ir_box is not None:
                ct_ir_only += 1
            elif a.eo_box is not None:
                ct_eo_only += 1

        logger.info("%d Box pairs added" % ct_both)
        logger.info("%d Annotations with only ir box added" % ct_ir_only)
        logger.info("%d Annotations with only eo box added" % ct_eo_only)
        s.commit()
        s.close()
        self.output().touch()
        x=1

### ==================== CHESS_ru Detections ====================
class Ingest_pb_ru_DetectionsTask(ForcibleTask):
    xml_directory = luigi.Parameter()

    def require(self):
        return Ingest_pb_ru_ImagesTask()

    def output(self):
        return SQLAlchemyCustomTarget(DATABASE_URI, 'LoadDetectionTask', self.task_id, echo=False)

    def cleanup(self):
        s = Session()
        eo_keys = s.query(EOImage.event_key).join(Camera, EOImage.camera).join(Flight, Camera.flight).join(Survey, Flight.survey).filter(Survey.name == 'CHESS_ru').all()
        eo_keys = [k for (k,) in eo_keys]
        ir_keys = s.query(IRImage.event_key).join(Camera, IRImage.camera).join(Flight, Camera.flight).join(Survey, Flight.survey).filter(Survey.name == 'CHESS_ru').all()
        ir_keys = [k for (k,) in ir_keys]
        for a in s.query(Annotation).filter(Annotation.ir_event_key.in_(ir_keys)).all(): s.delete(a)
        s.flush()
        for a in s.query(Annotation).filter(Annotation.eo_event_key.in_(eo_keys)).all(): s.delete(a)
        s.commit()
        s.close()
        self.output().remove()

    def run(self):
        logger = logging.getLogger('luigi-interface')

        xml_files = glob.glob(os.path.join(str(self.xml_directory), '*.xml'))
        image_files = glob.glob(os.path.join(str(self.xml_directory), '*.JPG'))
        assert (len(image_files) == len(xml_files))
        image_boxes = {}
        annotations = []
        for xml_file in xml_files:
            name, boxes = parse_VOC_xml(xml_file)
            assert (name not in image_boxes)

            dict_boxes = []
            for box in boxes:
                (id, xmin, ymin, xmax, ymax) = box
                dict_box = {}
                if id[:2] == 'PB':
                    dict_box['hsid'] = id
                    if id.endswith('s'):
                        logger.info('Skipped: %s' % id)
                        continue # skip shadow boxes
                else:
                    dict_box['hsid'] = None

                if 'Cub' in id:
                    dict_box['age_class'] = 'Cub'
                else:
                    dict_box['age_class'] = 'Adult'

                dict_box['x1'] = xmin
                dict_box['x2'] = xmax
                dict_box['y1'] = ymin
                dict_box['y2'] = ymax
                dict_boxes.append(dict_box)

            image_boxes[name] = dict_boxes

        species = add_species_if_not_exist(s, 'Polar Bear')

        orig_job = add_job_if_not_exists(s, 'Original_CHESS_ru_labels', '')
        orig_worker = add_worker_if_not_exists(s, 'Original_CHESS_ru_labeler', True)

        yuval_job = add_job_if_not_exists(s, 'labels_annotate_pb_chess_ru_2020-12-21', '')
        yuval_worker = add_worker_if_not_exists(s, 'Yuval', True)
        for image in image_boxes:
            boxes = image_boxes[image]
            for box in boxes:
                is_orig = box['hsid'] is not None
                job = orig_job if is_orig else yuval_job
                worker = orig_worker if is_orig else yuval_worker
                db_box = BoundingBox(x1=box['x1'],
                                     x2=box['x2'],
                                     y1=box['y1'],
                                     y2=box['y2'],
                                     worker = worker,
                                     job = job)
                s.add(db_box)
                s.flush()
                file_key = image.replace('.JPG', '')
                annot = Annotation(eo_event_key=file_key,
                                   ir_event_key=None,
                                   ir_box=None,
                                   eo_box=db_box,
                                   species=species,
                                   age_class=box['age_class'],
                                   is_shadow=False,
                                   hotspot_id=box['hsid']
                                   )
                s.add(annot)
                annotations.append(annot)
        s.commit()
        self.output().touch()
        logger.info('Ingested %d polar bear annotations' % len(annotations))
        s.close()
