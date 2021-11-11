from .base import Base
from .base import CommonQueries

from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import ExcludeConstraint
from sqlalchemy.dialects.postgresql import TSRANGE
from sqlalchemy import Index

from sqlalchemy import ForeignKey

from gtfs_builder.db.stops import StopsGeom


class StopsTimesValues(Base, CommonQueries):
    __table_args__ = (
        # ExcludeConstraint(('line_id', '='), ('trip_id', '='), ('pos', '&&')),
        # PrimaryKeyConstraint("stop_code", "date_time"),
        ExcludeConstraint(('stop_code', '='), ('study_area_name', '='), ('direction_id', '='), ('route_id', '='), ('trip_id', '='), ('validity_range', '&&')),
        Index('validity_range_index', "validity_range", postgresql_using='gist'),
        {'schema': 'gtfs_data'}
    )

    stop_code = Column(String, ForeignKey(StopsGeom.stop_code), index=True)
    study_area_name = Column(String, index=True)
    validity_range = Column(TSRANGE(), index=True)
    route_id = Column(String)
    trip_id = Column(String)
    direction_id = Column(String, index=True)