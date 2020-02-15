import psycopg2
from sqlalchemy.orm import sessionmaker



# Get queries
from noaadb.models import NOAAImage, Species, Job, Worker

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
