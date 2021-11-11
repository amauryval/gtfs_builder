from .base import Base
from .base import CommonQueries

from geoalchemy2 import Geometry

from sqlalchemy import Column
from sqlalchemy import String


from sqlalchemy.dialects.postgresql import ExcludeConstraint


class StopsGeom(Base, CommonQueries):
    __table_args__ = (
        ExcludeConstraint(('stop_code', '='), ('study_area_name', '!=')),
        {'schema': 'gtfs_data'}
    )
    id = Column(String, index=True, unique=True)
    stop_code = Column(String, index=True, unique=True)
    stop_name = Column(String)
    study_area_name = Column(String, index=True)
    geometry = Column(Geometry('POINT', 4326))
    route_type = Column(String)
    route_long_name = Column(String)
    route_short_name = Column(String)
