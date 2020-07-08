from sqlalchemy import and_

from noaadb.schema.models import Species, Job, Worker, Sighting, \
    Sighting, LabelEntry, IRLabelEntry, EOLabelEntry, Survey, Flight, Camera


# filter queries
# SELECT * FROM noaa_test.labels WHERE hotspot_id IS NULL;
def unidentified_labels(q):
    return q.filter_by(hotspot_id=None)
# Constrained gets
def get_existing_eo_label(session, label):
    return session.query(EOLabelEntry).filter_by(image_id=label.image_id,
                                                       x1=label.x1,
                                                       x2=label.x2,
                                                       y1=label.y1,
                                                       y2=label.y2).first()

def get_existing_ir_label(session, label):
    return session.query(IRLabelEntry).filter_by(image=label.image,
                                                       x1=label.x1,
                                                       x2=label.x2,
                                                       y1=label.y1,
                                                       y2=label.y2).first()

def get_existing_sighting(session, sighting):
    return session.query(Sighting).filter_by(eo_label=sighting.eo_label,
                                             ir_label=sighting.ir_label).first()
# def get_existing_falsepositive(session, hs):
#     return session.query(FalsePositiveLabels).filter_by(eo_label=hs.eo_label,
#                                                         ir_label=hs.ir_label).first()


# Get queries
def add_or_get_cam_flight_survey(s, c, f, survey):
    survey_obj = s.query(Survey).filter_by(name=survey).first()
    if not survey_obj:
        survey_obj = Survey(name=survey)
        s.add(survey_obj)
        s.flush()
    fl_obj = s.query(Flight).filter_by(flight_name = f).filter_by(survey_id=survey_obj.id).first()
    if not fl_obj:
        fl_obj = Flight(flight_name=f, survey_id=survey_obj.id)
        s.add(fl_obj)
        s.flush()
    cam_obj = s.query(Camera).filter_by(cam_name=c).filter_by(flight_id=fl_obj.id).first()
    if not cam_obj:
        cam_obj = Camera(cam_name=c, flight_id=fl_obj.id)
        s.add(cam_obj)
        s.flush()

    return cam_obj

# def get_image(session, name):
#     return  session.query(NOAAImage).filter_by(file_name=name).first()

def get_species(session, name):
    return  session.query(Species).filter_by(name=name).first()

def get_job_by_name(session, job_name):
    return  session.query(Job).filter_by(name=job_name).first()

def get_worker(session, name):
    return  session.query(Worker).filter_by(name=name).first()

# get all
def get_all_species(session):
    return  session.query(Species).all()

# exists queries
# def image_exists(session, name):
#     return session.query(session.query(NOAAImage).filter_by(file_name=name).deleted()).scalar()

def species_exists(session, name):
    return session.query(session.query(Species).filter_by(name=name).deleted()).scalar()

def job_exists(session, job_name):
    return session.query(session.query(Job).filter_by(name=job_name).deleted()).scalar()

def worker_exists(session, name):
    return session.query(session.query(Worker).filter_by(name=name).deleted()).scalar()

def label_exists(session, label):
    return session.query(session.query(LabelEntry)
                         .filter(LabelEntry.hotspot_id.like(str(label.hotspot_id)))
                         .filter_by(image=label.image,
                                    species=label.species,
                                    x1=label.x1,
                                    x2=label.x2,
                                    y1=label.y1,
                                    y2=label.y2).deleted()).scalar()

def add_job_if_not_exists(session, name, path):
    j = session.query(Job).filter_by(name=name).first()
    if not j:
        j = Job(
            name=name,
            file_path=path,
            notes=""
        )
        session.add(j)
        session.flush()

    return j

def add_worker_if_not_exists(session, name, is_human):
    w = get_worker(session, name)
    if not w:
        w = Worker(
            name=name,
            human=is_human
        )
        session.add(w)
        session.flush()

    return w

def add_worker_if_not_exists(session, name, is_human):
    w = get_worker(session, name)
    if not w:
        w = Worker(
            name=name,
            human=is_human
        )
        session.add(w)
        session.flush()

    return w