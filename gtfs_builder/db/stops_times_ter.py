from .base import Base
from .base import CommonQueries

from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy import Float
from sqlalchemy import DateTime

from sqlalchemy import Index
from geoalchemy2 import Geometry


class StopsTimesTer(Base, CommonQueries):
    __table_args__ = (
        # ExcludeConstraint(('line_id', '='), ('trip_id', '='), ('pos', '&&')),
        # PrimaryKeyConstraint("stop_code", "date_time"),
        # ExcludeConstraint(('stop_code', '='), ('study_area_name', '='), ('validity_range', '&&')),
        Index('idx_dates_ter', "start_date", "end_date", "geometry"),
        {'schema': 'gtfs_data'}
    )

    stop_code = Column(String, index=True)
    # study_area_name = Column(String(32), index=True)
    start_date = Column(DateTime(timezone=False))
    end_date = Column(DateTime(timezone=False))
    geometry = Column(Geometry('POINT', 4326))
    route_type = Column(String(32))
    stop_name = Column(String(32))
    pos = Column(Float, index=True)
    # route_long_name = Column(String)
    route_short_name = Column(String(32))
    direction_id = Column(String(12), index=True)