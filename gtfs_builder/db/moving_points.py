from datetime import datetime
from typing import Union
from typing import List

from .base import Base
from .base import CommonQueries

from sqlalchemy import Column, and_, func
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import TSRANGE, ExcludeConstraint

from sqlalchemy import Index
from geoalchemy2 import Geometry


class MovingPoints(Base, CommonQueries):
    __table_args__ = (
        # ExcludeConstraint(('line_id', '='), ('trip_id', '='), ('pos', '&&')),
        # PrimaryKeyConstraint("stop_code", "date_time"),
        ExcludeConstraint(('trip_id', '='), ('stop_code', '='), ('study_area', '='), ('validity_range', '&&')),
        Index('idx_dates_area_geometry', "validity_range", "study_area", "geometry"),
        Index('idx_geom_study_area', "geometry", "study_area"),
        {'schema': 'gtfs_data'}
    )

    stop_code = Column(String)
    study_area = Column(String(32), index=True)
    trip_id = Column(String(32))
    validity_range = Column(TSRANGE())
    geometry = Column(Geometry('POINT', 4326))
    route_type = Column(String(32))
    stop_name = Column(String(32))
    route_long_name = Column(String)

    @classmethod
    def filter_by_date_area(cls, date: datetime, area_name: str, geometry_bounds: Union[List[str], List[float]]):
        return cls._session.query(
            func.st_x(cls.geometry).label("x"),
            func.st_y(cls.geometry).label("y"),
            cls.route_long_name,
            cls.route_type
        ).filter(
            and_(
                cls.study_area == area_name,
                cls.validity_range.op('@>')(date),
                cls.geometry.intersects(func.st_makeenvelope(*geometry_bounds))
            )
        )

    @classmethod
    def get_bounds_by_area(cls, area_name: str):
        return cls._session.query(
            func.st_astext(func.st_extent(cls.geometry)).label("data_bounds"),
            func.min(func.lower(cls.validity_range)).label("start_date"),
            func.max(func.upper(cls.validity_range)).label("end_date"),
        ).filter(
            cls.study_area == area_name,
        ).group_by(
            cls.study_area
        )

    @classmethod
    def get_areas(cls):
        return cls._session.query(
            cls.study_area
        ).group_by(
            cls.study_area
        )