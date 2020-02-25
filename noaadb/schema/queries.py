from noaadb.noaadb.schema import NOAAImage, Species, Job, Worker, Label, Hotspot


# filter queries
# SELECT * FROM noaa_test.labels WHERE hotspot_id IS NULL;
def unidentified_labels(q):
    return q.filter_by(hotspot_id=None)
# Constrained gets
def get_existing_label(session, label):
    return session.query(Label).filter_by(image=label.image,
                                          species=label.species,
                                          x1=label.x1,
                                          x2=label.x2,
                                          y1=label.y1,
                                          y2=label.y2).first()

def get_existing_hotspot(session, hs):
    return session.query(Hotspot).filter_by(eo_label=hs.eo_label,
                                          ir_label=hs.ir_label).first()
# Get queries
def get_image(session, name):
    return  session.query(NOAAImage).filter_by(file_name=name).first()

def get_species(session, name):
    return  session.query(Species).filter_by(name=name).first()

def get_job_by_name(session, job_name):
    return  session.query(Job).filter_by(job_name=job_name).first()

def get_worker(session, name):
    return  session.query(Worker).filter_by(name=name).first()

# get all
def get_all_species(session):
    return  session.query(Species).all()

# exists queries
def image_exists(session, name):
    return session.query(session.query(NOAAImage).filter_by(file_name=name).exists()).scalar()

def species_exists(session, name):
    return session.query(session.query(Species).filter_by(name=name).exists()).scalar()

def job_exists(session, job_name):
    return session.query(session.query(Job).filter_by(job_name=job_name).exists()).scalar()

def worker_exists(session, name):
    return session.query(session.query(Worker).filter_by(name=name).exists()).scalar()

def label_exists(session, label):
    return session.query(session.query(Label)
                         .filter(Label.hotspot_id.like(str(label.hotspot_id)))
                         .filter_by(image=label.image,
                                    species=label.species,
                                    x1=label.x1,
                                    x2=label.x2,
                                    y1=label.y1,
                                    y2=label.y2).exists()).scalar()

def add_job_if_not_exists(session, name, path):
    j = get_job_by_name(session, name)
    if not job_exists(session, name):
        j = Job(
            job_name=name,
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