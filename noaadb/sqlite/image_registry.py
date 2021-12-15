from tokenize import String

import sqlalchemy as sa
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class RawImages(Base):
    __tablename__ = 'raw_images'
    id = Column(Integer, autoincrement=True, primary_key=True)
    filename = Column(String(200), unique=True)
    width = Column(Integer)
    height = Column(Integer)
    channels= Column(Integer)


class SqliteImageRegistry():
    def __init__(self, absolute_file_path):
        self.engine = sa.create_engine('sqlite:///' + absolute_file_path)
        self.Session = sessionmaker(bind=self.engine)
        self.create()
        self.new = []

    def create(self):
        Base.metadata.create_all(self.engine)

    def add(self, filename, width, height, channels):
        self.new.append(RawImages(filename=filename, width=width, height=height, channels=channels))

    def commit(self):
        s = self.Session()
        while len(self.new) > 0:
            el = self.new.pop()
            if s.query(RawImages).filter(RawImages.filename == el.filename).first() is None:
                s.add(el)
        s.commit()
        s.close()

    def to_dict(self):
        s = self.Session()
        res = s.query(RawImages).all()
        s.close()
        d = {}
        for im in res:
            d[im.filename] = im
        return d
