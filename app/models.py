from typing import Dict
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, DateTime

from paths import path_DB

path_to_DB: str = 'sqlite:///' + str(path_DB)

engine = create_engine(url=path_to_DB)

Base = declarative_base(bind=engine)
Session = sessionmaker(bind=engine)
session = Session()


class BaseTable(Base):
    __abstract__ = True

    id = Column(Integer, primary_key=True)

    def to_json(self) -> Dict:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def __repr__(self):
        return '{attr}'.format(attr={c.name: getattr(self, c.name) for c in self.__table__.columns})


class Matrix(BaseTable):
    __tablename__ = 'matrix'

    ts = Column(DateTime)
    departure_zid = Column(Integer)
    arrival_zid = Column(Integer)
    customers_cnt = Column(Integer)
    customers_cnt_metro = Column(Integer)


class Zones(BaseTable):
    __tablename__ = 'zones'

    zone = Column(Integer)
    district = Column(Integer)
