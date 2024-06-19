import datetime
from sqlalchemy import Column, Integer, Float, DateTime
from .base import Base


class Epi(Base):
    __tablename__ = "epi"

    id = Column(Integer, primary_key=True)
    datetime = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    metric1 = Column(Float, nullable=False)

    def __init__(self, metric1):
        self.metric1 = metric1

    def __repr__(self):
        return f"<Epi(metric1={self.metric1})>"
