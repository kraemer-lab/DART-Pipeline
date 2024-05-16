import datetime
from sqlalchemy import Column, Integer, Float, DateTime
from .base import Base


class Climate(Base):
    __tablename__ = "climate"

    id = Column(Integer, primary_key=True)
    datetime = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    temperature = Column(Float)

    def __init__(self, latitude, longitude, temperature):
        self.latitude = latitude
        self.longitude = longitude
        self.temperature = temperature

    def __repr__(self):
        return f"<Climate {self.id}>"
