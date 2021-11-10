from .base import Base
from .base import CommonQueries

from geoalchemy2 import Geometry

from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy import DateTime, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import DATE
from sqlalchemy.dialects.postgresql import ExcludeConstraint
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import TSRANGE
from sqlalchemy import Index

from sqlalchemy import ForeignKey

from gtfs_builder.db.stops import StopsGeom


class StopsTimesValues(Base, CommonQueries):
    __table_args__ = (
        # ExcludeConstraint(('line_id', '='), ('trip_id', '='), ('pos', '&&')),
        # PrimaryKeyConstraint("stop_code", "date_time"),
        ExcludeConstraint(('stop_code', '='), ('study_area_name', '='), ('direction_id', '='), ('line_id', '='), ('trip_id', '='), ('validity_range', '&&')),
        Index('validity_range_index', "validity_range", postgresql_using='gist'),
        {'schema': 'gtfs_data'}
    )

    stop_code = Column(String, ForeignKey(StopsGeom.stop_code), index=True)
    study_area_name = Column(String, index=True)
    validity_range = Column(TSRANGE(), index=True)
    line_id = Column(String)
    trip_id = Column(String)
    direction_id = Column(String, index=True)