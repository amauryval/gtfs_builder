from .base import Base
from .base import CommonQueries

from geoalchemy2 import Geometry

from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy import DateTime, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import DATE
from sqlalchemy.dialects.postgresql import ExcludeConstraint
from sqlalchemy import PrimaryKeyConstraint


class StopsGeom(Base, CommonQueries):
    __table_args__ = (
        # ExcludeConstraint(('line_id', '='), ('trip_id', '='), ('pos', '&&')),
        {'schema': 'gtfs_data'}
    )
    id = Column(String, index=True, unique=True)
    stop_code = Column(String, index=True, unique=True)
    stop_name = Column(String, index=True)
    geometry = Column(Geometry('POINT', 4326))
    pos = Column(String, index=True)
    stop_type = Column(String, index=True)
    line_name = Column(String, index=True)
    line_name_short = Column(String, index=True)

