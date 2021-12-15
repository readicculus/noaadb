import os

import cv2
import luigi
from core.tools.deps_tree import print_tree
from noaadb import Session
from noaadb.schema.models import *
from pipelines.ingest.tasks import Ingest_pb_ru_ImagesTask, Ingest_pb_ru_DetectionsTask, Ingest_pb_us_ImagesTask, \
    Ingest_pb_us_DetectionsTask, Ingest_pb_beufort_19_DetectionsTask

if __name__ == '__main__':
    # print(print_tree(Ingest_pb_ru_DetectionsTask()))
    # s = Session()
    # eo_pb_count = s.query(Annotation).join(Species).filter(Species.name == "Polar Bear").filter(Annotation.eo_event_key != None).count()
    # ir_pb_count = s.query(Annotation).join(Species).filter(Species.name == "Polar Bear").filter(Annotation.ir_event_key != None).count()
    # s.close()
    # a=1
    # luigi.build([Ingest_pb_ru_DetectionsTask()], detailed_summary=True, local_scheduler=True)
    # luigi.build([Ingest_pb_beufort_19_DetectionsTask()], detailed_summary=True, local_scheduler=True)


    # s = Session()
    # images = s.query(EOImage) \
    #     .join(Camera, EOImage.camera).join(Flight, Camera.flight).join(Survey, Flight.survey).filter(
    #     Survey.name == '2019_beaufort').all()
    # for im in images:
    #     x = im.draw_annotations()
    #     a=1
    # s.close()

    s = Session()
    images = s.query(EOImage).join(Annotation, Annotation.eo_event_key == EOImage.event_key) \
        .join(Camera, EOImage.camera).filter(Camera.cam_name == 'C')\
        .join(Flight, Camera.flight).filter(Flight.flight_name == 'fl05')\
        .join(Survey, Flight.survey).filter(Survey.name == 'test_kotz_2019')\
        .distinct(EOImage.event_key).all()
    eo_list = []
    ir_list = []
    for eo_im in images:
        ir_im = s.query(IRImage).filter(IRImage.event_key == eo_im.event_key).first()
        if ir_im:
            eo_list.append(eo_im.path())
            ir_list.append(ir_im.path())
            # ir_path = os.path.join('/fast/generated_data/viame', ir_im.filename).replace('.tif','.jpg')
            # im = ir_im.ocv_load_normed()
            # if im.shape != (512,640):
            #     continue
            # cv2.imwrite(ir_path, im)
            # ir_list.append(ir_path)

    with open('/data/software/seal_tk/eo_list_tif.txt', 'w') as f:
        for im in eo_list:
            f.write('%s\n'%im)
    with open('/data/software/seal_tk/ir_list_tif.txt', 'w') as f:
        for im in ir_list:
            f.write('%s\n'%im)
    x = 1
    s.close()

# make pb csv
    # s = Session()
    # x=s.query(EOImage, IRImage).join(IRImage, EOImage.event_key == IRImage.event_key) \
    #     .join(Camera, EOImage.camera)\
    #     .filter(Camera.cam_name == 'C')\
    #     .join(Flight, Camera.flight) \
    #     .filter(Flight.flight_name == 'fl05') \
    #     .join(Survey, Flight.survey)\
    #     .filter(Survey.name == 'test_kotz_2019')\
    #     .all()
    # s.close()
    # EO_FILE = '/data/software/viame/src/examples/object_detection/eo_list.txt'
    # IR_FILE = '/data/software/viame/src/examples/object_detection/ir_list.txt'
    # out = '/data2/2019/fl05/PNG'
    # with open(EO_FILE, 'w') as eo_f:
    #     with open(IR_FILE, 'w') as ir_f:
    #         for eo, ir in x:
    #             im = ir.ocv_load_normed()
    #             ir_path = os.path.join(out, ir.filename).replace('.tif', '.png')
    #             cv2.imwrite(ir_path, im)
    #             eo_f.write(eo.path() + '\n')
    #             ir_f.write(ir_path + '\n')

    a=1
