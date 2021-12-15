import os

import luigi
import pandas as pd

from core import ForcibleTask, SQLAlchemyCustomTarget
from pipelines.ingest.tasks import IngestCHESSImagesTask
from pipelines.ingest.util.image_utilities import file_key


from noaadb import DATABASE_URI, Session
from noaadb.schema.models.survey_data import *
from noaadb.schema.models.annotation_data import *
from noaadb.schema.utils.queries import add_species_if_not_exist, add_worker_if_not_exists, add_job_if_not_exists



class CleanCHESSDetectionsTask(ForcibleTask):
    output_root = luigi.Parameter()
    detection_csv = luigi.Parameter()

    def requires(self):
        yield IngestCHESSImagesTask()

    def output(self):
        fn = 'clean_%s' % os.path.basename(str(self.detection_csv))
        fp_csv = os.path.join(str(self.output_root), fn)
        return luigi.LocalTarget(fp_csv)

    def cleanup(self):
        if self.output().exists():
            self.output().remove()

    def run(self):
        fog_converter = lambda x: False if x == 'No' or pd.isna(x) else True
        confidence_converter = lambda x: 'Likely' if x == '100%' else None if pd.isna(x) or x == 'NA' else x
        none_if_na = lambda x: 'None' if pd.isna(x) else x
        converters = {'event_key_eo': file_key,
                      'event_key_ir': file_key,
                      'is_fog': fog_converter,
                      'species_confidence': confidence_converter,
                      'x1_ir': none_if_na,
                      'y1_ir': none_if_na
                      }
        columns = ['id', 'event_key_eo', 'event_key_ir', 'hotspot_id', 'hotspot_type',
                   'species', 'species_confidence', 'is_fog',
                   'x1_ir', 'y1_ir',
                   'old_x1_eo', 'old_y1_eo', 'old_x2_eo', 'old_y2_eo',
                   'x1_eo', 'y1_eo', 'x2_eo', 'y2_eo',
                   'updated', 'status']

        df = pd.read_csv(str(self.detection_csv), comment='#', header=0,
                         names=columns, index_col=False, converters=converters)

        # set all new as 'Maybe' confidence
        df.loc[df['status'].str.contains('new'), ['species_confidence']] = 'Maybe'

        #  'removed', 'none', 'off_edge', 'bad_res', 'maybe_seal', 'maybe_seal-bad_res', 'bad_res-maybe_seal',
        #  'bad_res-off_edge', 'new-bad_res', 'new', 'new-maybe_seal', 'new-bad_res-maybe_seal', 'new-off_edge',
        #  'new-maybe_seal-off_edge', 'new-maybe_seal-bad_res'

        new_rows = []
        for i, row in df.iterrows():

            status = row.status.split('-')
            updated = row.updated

            if 'removed' in status:
                continue

            new_row = row.copy()
            new_row.drop(['old_x1_eo', 'old_y1_eo', 'old_x2_eo', 'old_y2_eo'], inplace=True)
            new_row = new_row.to_dict()

            new_row['x1_eo'] = row.x1_eo if updated else row.old_x1_eo
            new_row['y1_eo'] = row.y1_eo if updated else row.old_y1_eo
            new_row['x2_eo'] = row.x2_eo if updated else row.old_x2_eo
            new_row['y2_eo'] = row.y2_eo if updated else row.old_y2_eo
            new_row['x2_ir'] = row.x1_ir
            new_row['y2_ir'] = row.y1_ir
            possible_statuses = ['removed', 'off_edge', 'bad_res', 'maybe_seal', 'new']
            for s in possible_statuses:
                new_row['flag_%s' % s] = True if s in status else False

            new_rows.append(new_row)

        new_df = pd.DataFrame(new_rows)
        with self.output().open('w') as f:
            new_df.to_csv(f, index=False)


class IngestCHESSDetectionsTask(ForcibleTask):
    detection_csv = luigi.Parameter()
    survey = luigi.Parameter()

    def requires(self):
       return { 'ingest_chess_images': IngestCHESSImagesTask(),
                'cleaned_csv': CleanCHESSDetectionsTask(detection_csv=self.detection_csv)}

    # make flag in db that this detection file was loaded
    def output(self):
        return SQLAlchemyCustomTarget(DATABASE_URI, 'LoadKotzDetectionsTask', os.path.basename(str(self.detection_csv)),
                                     echo=False)

    def cleanup(self):
        # Cleans up/deletes all Annotation and BoundingBox objects associated with this task's flight and cam id
        survey = self.survey

        s = Session()
        cam_ids = s.query(Camera.id).join(Flight).join(Survey).filter(Survey.name == survey).all()
        annotations = s.query(Annotation).join(IRImage, IRImage.event_key == Annotation.ir_event_key)\
            .filter(IRImage.camera_id.in_(cam_ids)).all()
        annotations += s.query(Annotation).join(EOImage, EOImage.event_key == Annotation.eo_event_key)\
            .filter(EOImage.camera_id.in_(cam_ids)).all()
        to_remove = list({a.id: a for a in annotations}.keys())
        boxes_to_remove = []
        for x in annotations:
            boxes_to_remove.append(x.ir_box_id)
            boxes_to_remove.append(x.eo_box_id)
        if len(to_remove) > 0:
            removed = s.query(BoundingBox).filter(BoundingBox.id.in_(boxes_to_remove)).delete(synchronize_session=False)
            s.commit()
            removed = s.query(Annotation).filter(Annotation.id.in_(to_remove)).delete(synchronize_session=False)
            s.commit()

        s.flush()
        # verify none
        annotations = s.query(Annotation).join(IRImage, IRImage.event_key == Annotation.ir_event_key)\
            .filter(IRImage.camera_id.in_(cam_ids)).all()
        annotations += s.query(Annotation).join(EOImage, EOImage.event_key == Annotation.eo_event_key)\
            .filter(EOImage.camera_id.in_(cam_ids)).all()
        assert (len(annotations) == 0)
        s.close()
        self.output().remove()

    def run(self):
        cleaned_csv = self.input()['cleaned_csv']
        with cleaned_csv.open('r') as f:
            df = pd.read_csv(f)

        s = Session()
        species = list(df.species.unique())
        species_dict = {}
        for sp in species:
            species_dict[sp] = add_species_if_not_exist(s, sp)

        new_worker = add_worker_if_not_exists(s, 'Yuval', True)
        updated_worker = add_worker_if_not_exists(s, 'Gavin -> Yuval', True)
        old_worker = add_worker_if_not_exists(s, 'Gavin', True)
        old_job = add_job_if_not_exists(s, 'chess_original', '')
        updated_job = add_job_if_not_exists(s, 'chess_yuval_relabel', '')

        for index, row in df.iterrows():
            worker = new_worker if row.flag_new else updated_worker if row.updated else old_worker
            job = updated_job if row.updated else old_job

            species_obj = species_dict[row.species]
            eo_box = BoundingBox(x1=row.x1_eo,
                                 x2=row.x2_eo,
                                 y1=row.y1_eo,
                                 y2=row.y2_eo,
                                 confidence=row.species_confidence,
                                 worker=worker,
                                 job=job)

            ir_box = None
            if -1 not in [row.x1_ir, row.x2_ir, row.y1_ir, row.y2_ir]:
                ir_box = BoundingBox(x1=row.x1_ir,
                                     x2=row.x2_ir,
                                     y1=row.y1_ir,
                                     y2=row.y2_ir,
                                     confidence=row.species_confidence,
                                     worker=worker,
                                     job=job)
                s.add(ir_box)
            s.add(eo_box)
            s.flush()
            annot = Annotation(eo_event_key=row.event_key_eo,
                               ir_event_key=row.event_key_ir if ir_box is not None else None,
                               ir_box=ir_box,
                               eo_box=eo_box,
                               species=species_obj,
                               is_shadow=False,
                               hotspot_id=row.hotspot_id
                               )
            s.add(annot)
            s.flush()
        s.commit()
        s.close()
        self.output().touch()
