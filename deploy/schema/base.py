from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()


class Database:
    def __init__(self, connection):
        self.engine = create_engine(connection)
        self.Session = sessionmaker(bind=self.engine)

    def init_tables(self):
        Base.metadata.create_all(self.engine)

    def drop_tables(self):
        Base.metadata.drop_all(self.engine)

    def get_session(self):
        return self.Session()
