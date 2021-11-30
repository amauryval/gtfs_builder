from .base import Base
from sqlalchemy import Index
from geoalchemy2 import Geometry

from sqlalchemy import Column, Integer, DateTime, String, Float

class StopsTimesTer(Base):
    __table_args__ = (
        Index('idx_dates_ter', "start_date", "end_date"),
    )
    uuid = Column(Integer, primary_key=True)
    stop_code = Column(String, index=True)
    # study_area_name = Column(String(32), index=True)
    start_date = Column(DateTime(timezone=False))
    end_date = Column(DateTime(timezone=False))
    geom = Column(Geometry(geometry_type='POINT', management=True, srid=4326))
    route_type = Column(String(32))
    stop_name = Column(String(32))
    pos = Column(Float, index=True)
    # route_long_name = Column(String)
    route_short_name = Column(String(32))
    direction_id = Column(String(12), index=True)

