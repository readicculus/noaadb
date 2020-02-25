import os

from sqlalchemy import create_engine, and_, not_, or_
from sqlalchemy.orm import sessionmaker, aliased
from noaadb.schema.models import Label, NOAAImage, Worker, Species, Hotspot

class LabelDBApi:
    """
        A basic public api for querying the label database.
        This class allows to execute custom pgsql statements but if you want
        to do something that this class does not provide functionality for using the awesome sqlalchemy
        serialization features you can just manage the session on your own.
        Session = api.get_session_config()
        session = Session()
    """
    def __init__(self):

        self.config = {
            "db_password": "readonly",
            "db_user": "readonly",
            "db_name": "noaa",
            "db_host": "yuvalboss.com",
            "schema_name": "noaa_test" if "DEBUG" in os.environ and os.environ["DEBUG"] else "noaa"
        }

        self.DATABASE_URI = 'postgres+psycopg2://%s:%s@%s:5432/noaa' % \
                            (self.config["db_user"], self.config["db_password"], self.config["db_host"])
        self.noaa_engine = create_engine(self.DATABASE_URI)
        self.Session = sessionmaker(bind=self.noaa_engine)
        self.session = None

    def get_session_config(self):
        return self.Session

    def begin_session(self):
        if self.session is not None:
            return False
        self.session = self.Session()
        return True

    def close_session(self):
        if self.session is None:
            return False
        self.session.close()
        return True

    def execute(self, sql):
        self.verify_session()
        return self.session.execute(sql)

    def get_hotspots(self, species_filter = None):
        """
        Get all Hotspots with corresponding IR & RGB Labels, Images, workers, jobs, species etc...

        Parameters:
           species_filter (:obj:`list` of :obj:`str`): if not provided

        :return: a list of two element arrays [:class: `.Hotspot`]
        """
        res = self.session.query(Hotspot)

        if species_filter is not None:
            res = res\
                .join(Label, Hotspot.eo_label)\
                .join(Species, Label.species)\
                .filter(Species.name.in_(species_filter))
        return res.all()


    def get_eo_labels(self, without_shadows = True,
                      in_species=('Polar Bear', 'Ringed Seal', 'Bearded Seal', 'UNK Seal'),
                      verification_only = False):
        """
        Get RGB Label records, with corresponding Hotspot if exists otherwise will be None

        Parameters:
           without_shadows (bool): if true does not return records that are labels of shadows.
           in_species (:obj:`list` of :obj:`str`): return only records in the given species.
           verification_only (bool): only return records that need verification.

        :return: a list of two element arrays [:class:`.Label`, :class: `.Hotspot`]
        """
        self.verify_session()
        eo_image = aliased(NOAAImage)
        eo_worker = aliased(Worker)
        species = aliased(Species)

        results = self.session.query(Label, Hotspot) \
                .outerjoin(Hotspot, Hotspot.eo_label_id == Label.id) \
                .join(species, Label.species) \
                .join(eo_worker, Label.worker) \
                .join(eo_image, Label.image) \
                .join(Label.job) \
                .filter(
                    and_(
                        eo_image.type == 'RGB',
                        species.name.in_(in_species)
                    )
                )
        if without_shadows:
            results = results.filter(not_(Label.is_shadow))

        if verification_only:
            results = results.filter(or_(
                eo_worker.name == 'noaa',   # original noaa label
                Label.end_date != None,     # end_date exists if label was removed
                Label.x1 < 0,               # out of bounds
                Label.x2 > eo_image.width,  # out of bounds
                Label.y1 < 0,               # out of bounds
                Label.y2 > eo_image.height, # out of bounds
                Label.x1 == None,
                Label.x2 == None,
                Label.y1 == None,
                Label.y2 == None,
            ))

        return results.all()

    def get_all_species(self):
        return self.session.query(Species).all()

    def verify_session(self):
        if self.session is None:
            raise Exception("No current session")