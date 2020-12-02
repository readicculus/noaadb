import glob
import json
import logging
import os

import luigi
from luigi.contrib import sqla

from ingest.tasks import CreateTableTask
from ingest.tasks import AggregateKotzImagesTask
from ingest.util import file_key, flight_cam_id_from_dir
from noaadb import Session, DATABASE_URI
from noaadb.schema.models import EOImage, IRImage, BoundingBox, Annotation, Camera, Flight, Survey
import pandas as pd
import numpy as np

from noaadb.schema.utils.queries import add_worker_if_not_exists, add_job_if_not_exists, add_species_if_not_exist


class JoinDirectoryEOIRCSVTask(luigi.Task):
    directory = luigi.Parameter()
    output_root = luigi.Parameter()
    flight_id = luigi.Parameter()
    cam_id = luigi.Parameter()

    def output(self):
        fn_csv = '%s_%s_merged.csv' % (self.flight_id, self.cam_id)
        fp_csv = os.path.join(str(self.output_root), fn_csv)
        fn_json = '%s_%s_info.json' % (self.flight_id, self.cam_id)
        fp_json = os.path.join(str(self.output_root), fn_json)
        return {'csv': luigi.LocalTarget(fp_csv), 'json': luigi.LocalTarget(fp_json)}

    def load_dataframe(self, csv_path: str, suffix: str) -> pd.DataFrame:
        # VIAME Detection CSV format
        # 1: Detection or Track-id
        # 2: Video or Image Identifier
        # 3: Unique Frame Identifier
        # 4-7: Img-bbox(TL_x	TL_y	BR_x	BR_y)
        # 8: Detection or Length Confidence
        # 9: Target Length (0 or -1 if invalid)
        # 10-11+: Repeated Species	 Confidence Pairs or Attributes

        columns = ['detection_id', 'image_id', 'frame_id', 'x1', 'y1', 'x2', 'y2',
                   'confidence', 'target_length', 'species', 'species_confidence']
        types = [np.int, str, np.int32, np.float64, np.float64, np.float64, np.float64,
                 np.float64, np.int32, str, np.float64]
        columns = [str(col) + '_' + suffix for col in columns]
        dtypes = {a:b for a,b in zip(columns,types)}
        converters = {columns[1]: file_key}
        del dtypes[columns[1]]
        df = pd.read_csv(csv_path, header=None, comment='#', index_col=False,
                         names=columns, dtype=dtypes, converters=converters)

        return df

    def run(self):
        logger = logging.getLogger('luigi-interface')

        ir_csv_files = glob.glob(os.path.join(str(self.directory), '*_ir_*.csv'))
        eo_csv_files = glob.glob(os.path.join(str(self.directory), '*_eo_*.csv'))
        assert (len(ir_csv_files) < 2)
        assert (len(eo_csv_files) < 2)
        ir_csv = ir_csv_files[0] if len(ir_csv_files) == 1 else None
        eo_csv = ir_csv_files[0] if len(eo_csv_files) == 1 else None
        if eo_csv is None or ir_csv is None:
            message = 'No eo or ir csv found.' if not eo_csv and not ir_csv else \
                'No eo csv found.' if not eo_csv else 'No ir csv found.'
            logger.error(message)
            raise Exception(message)

        eo_df = self.load_dataframe(eo_csv, 'eo')
        ir_df = self.load_dataframe(ir_csv, 'ir')
        merged = pd.merge(left=eo_df, right=ir_df,
                          left_on='detection_id_eo', right_on='detection_id_ir',
                          how='outer', validate='one_to_one')
        col_map = {'detection_id_eo': 'detection_id',
                   'image_id_eo': 'event_key',
                   'frame_id_eo': 'frame_id',
                   'confidence_eo': 'confidence',
                   'species_eo': 'species',
                   'species_confidence_eo': 'species_confidence'}
        cols_remove = ['confidence_ir', 'target_length_ir', 'species_ir', 'species_confidence_ir',
                       'detection_id_ir', 'image_id_ir', 'frame_id_ir', 'target_length_eo']
        merged.rename(columns=col_map, inplace=True)
        merged.drop(cols_remove, axis=1, inplace=True)
        merged.species.fillna('UNK', inplace=True)
        merged = merged.loc[merged['species'] != 'incorrect']
        logger.info('Saving %d Annotations to csv %s.' % (len(merged), self.output()['csv'].path))
        with self.output()['csv'].open('w') as f:
            merged.to_csv(f, index=False)

        with self.output()['json'].open('w') as f:
            f.write(json.dumps({'flight_id': self.flight_id, 'cam_id': self.cam_id, 'annotation_count': len(merged)}))


# class AggregateKotzDetectionsTask(luigi.Task):
#     image_directories = luigi.ListParameter()
#     output_directory = luigi.Parameter()
#
#     def requires(self):
#         req = {}
#         for directory in list(self.image_directories):
#             req[directory] = JoinDirectoryEOIRCSVTask(directory=directory, output_directory=self.output_directory)
#         return req
#
#     def output(self):
#         return self.input()


class LoadKotzDetectionsTask(luigi.Task):
    directory = luigi.Parameter()
    survey = luigi.Parameter()
    species_map = {'unknown_seal': 'UNK Seal',
                   'unknown_pup': 'UNK Seal',
                   'ringed_seal': 'Ringed Seal',
                   'ringed_pup': 'Ringed Seal',
                   'bearded_seal': 'Bearded Seal',
                   'bearded_pup': 'Bearded Seal',
                   'animal': 'animal',
                   'Ringed Seal': 'Ringed Seal',
                   'Bearded Seal': 'Bearded Seal',
                   'Polar Bear': 'Polar Bear',
                   'incorrect': 'falsepositive',
                   'UNK': 'UNK'}
    def requires(self):
        yield CreateTableTask(
            children=["Job", "Worker", "Species", "BoundingBox", "Annotation"])
        flight_id, cam_id = flight_cam_id_from_dir(str(self.directory))
        yield JoinDirectoryEOIRCSVTask(directory=self.directory, flight_id=flight_id, cam_id=cam_id)

    # make flag in db that this detection file was loaded
    def output(self):
        return sqla.SQLAlchemyTarget(DATABASE_URI, 'LoadKotzDetectionsTask', os.path.basename(str(self.input()[1]['csv'].path)), echo=False)

    def cleanup(self):
        # Cleans up/deletes all Annotation and BoundingBox objects associated with this task's flight and cam id
        flight_id, cam_id = flight_cam_id_from_dir(str(self.directory))
        survey = self.survey

        s = Session()
        cam_obj = s.query(Camera)\
            .filter(Camera.cam_name == cam_id)\
            .join(Flight).filter(Flight.flight_name == flight_id)\
            .join(Survey).filter(Survey.name == survey).first()
        annotations = s.query(Annotation).join(IRImage, IRImage.event_key == Annotation.ir_event_key)\
            .filter(IRImage.camera_id == cam_obj.id).all()
        annotations += s.query(Annotation).join(EOImage, EOImage.event_key == Annotation.eo_event_key)\
            .filter(EOImage.camera_id == cam_obj.id).all()
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
        annotations = s.query(Annotation).join(IRImage, IRImage.event_key == Annotation.ir_event_key) \
            .filter(IRImage.camera_id == cam_obj.id).all()
        annotations += s.query(Annotation).join(EOImage, EOImage.event_key == Annotation.eo_event_key) \
            .filter(EOImage.camera_id == cam_obj.id).all()
        assert (len(annotations) == 0)
        s.close()

    def run(self):
        self.cleanup()
        s = Session()
        eo_worker = add_worker_if_not_exists(s, 'Gavin', True)
        job = add_job_if_not_exists(s, 'kotz_manual_review', '')
        ir_worker = add_worker_if_not_exists(s, 'Projected', False)
        merged_csv_fp = self.input()[1]['csv'].path
        flight_cam_str = os.path.basename(str(merged_csv_fp)).replace('_merged.csv', '')
        df = pd.read_csv(str(merged_csv_fp))

        # filter out incorrects
        df = df.loc[df['species'] != 'incorrect']

        # get unique species and add them to db #todo
        species_raw = list(df.species.unique())
        species = [self.species_map[sp] for sp in species_raw]
        species_dict = {}
        for sp in species:
            species_dict[sp] = add_species_if_not_exist(s, sp)

        for index, row in df.iterrows():
            species_obj = species_dict[self.species_map[row.species]]
            eo_box = BoundingBox(x1=row.x1_eo,
                                 x2=row.x2_eo,
                                 y1=row.y1_eo,
                                 y2=row.y2_eo,
                                 confidence=row.confidence,
                                 worker = eo_worker,
                                 job = job)
            ir_box = BoundingBox(x1=row.x1_ir,
                                 x2=row.x2_ir,
                                 y1=row.y1_ir,
                                 y2=row.y2_ir,
                                 confidence=row.confidence,
                                 worker = ir_worker,
                                 job = job)
            s.add(eo_box)
            s.add(ir_box)
            s.flush()
            is_pup = not pd.isnull(row.species) and 'pup' in row.species
            annot = Annotation(eo_event_key=row.event_key,
                               ir_event_key=row.event_key,
                               ir_box=ir_box,
                               eo_box=eo_box,
                               species=species_obj,
                               age_class='pup' if is_pup else None,
                               is_shadow=False,
                               hotspot_id='%s_%d' % (flight_cam_str, row.detection_id)
                               )
            s.add(annot)
            s.flush()
        s.commit()
        s.close()
        self.output().touch()

class IngestKotzDetectionsTask(luigi.Task):
    image_directories = luigi.ListParameter()
    survey = luigi.Parameter()

    def requires(self):
        req = {}
        yield AggregateKotzImagesTask()
        for directory in list(self.image_directories):
            req[directory] = LoadKotzDetectionsTask(directory=directory, survey = self.survey)
        yield req

    def output(self):
        return self.input()



