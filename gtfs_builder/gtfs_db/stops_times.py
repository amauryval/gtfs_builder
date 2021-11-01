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

from sqlalchemy import ForeignKey
from sqlalchemy import Index

from gtfs_builder.gtfs_db.stops import StopsGeom

class StopsTimesValues(Base, CommonQueries):
    __table_args__ = (
        # ExcludeConstraint(('line_id', '='), ('trip_id', '='), ('pos', '&&')),
        # PrimaryKeyConstraint("stop_code", "date_time"),
        Index('validity_range_index', "validity_range", postgresql_using='gist'),
        ExcludeConstraint(('stop_code', '='), ('direction_id', '='), ('line_id', '='), ('trip_id', '='), ('validity_range', '&&')),
        {'schema': 'gtfs_data'}
    )

    stop_code = Column(String, ForeignKey(StopsGeom.stop_code), index=True)
    validity_range = Column(TSRANGE())
    line_id = Column(String)
    trip_id = Column(String)
    direction_id = Column(String, index=True)
