import datetime
from sqlalchemy import Column, Integer, Float, DateTime, String
from .base import Base


class Climate(Base):
    __tablename__ = "climate"

    id = Column(Integer, primary_key=True)
    datetime = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    gid2 = Column(String, nullable=False)
    rainfall = Column(Float)

    def __init__(self, gid2, rainfall):
        self.gid2 = gid2
        self.rainfall = rainfall

    def __repr__(self):
        return f"<Climate {self.id}>"
