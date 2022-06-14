from .base import Base
from .base import CommonQueries

from sqlalchemy import Column, and_
from sqlalchemy import String
from sqlalchemy import Float
from sqlalchemy.dialects.postgresql import TSRANGE, ExcludeConstraint

from sqlalchemy import Index
from geoalchemy2 import Geometry


class MovingPoints(Base, CommonQueries):
    __table_args__ = (
        # ExcludeConstraint(('line_id', '='), ('trip_id', '='), ('pos', '&&')),
        # PrimaryKeyConstraint("stop_code", "date_time"),
        ExcludeConstraint(('trip_id', '='), ('stop_code', '='), ('study_area', '='), ('validity_range', '&&')),
        Index('idx_dates_ter', "validity_range", "study_area"),
        {'schema': 'gtfs_data'}
    )

    stop_code = Column(String)
    study_area = Column(String(32))
    trip_id =  Column(String(32))
    validity_range = Column(TSRANGE())
    geometry = Column(Geometry('POINT', 4326))
    route_type = Column(String(32))
    stop_name = Column(String(32))
    pos = Column(Float)
    route_long_name = Column(String)
    route_short_name = Column(String(32))
    direction_id = Column(String(12))


    @classmethod
    def filter_by_date_area(cls, date, area_name):
        return cls._session.query(
            cls
        ).filter(
            and_(
                cls.study_area == area_name,
                cls.validity_range.op('@>')(date),
            )
        )
