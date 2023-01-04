from typing import List, Dict

from sqlalchemy.orm import Session
from starlite import Router, Controller, get, Provide

from gtfs_builder.app.global_values import date_format
from gtfs_builder.app.session import get_session

from gtfs_builder.db import MovingPoints

from shapely.wkt import loads

import datetime


def set_moving_nodes_session(session: Session):
    MovingPoints.set_session(session)


class FromDbController(Controller):

    @get("/existing_study_areas")
    def existing_study_areas(self, session: Session) -> List[str]:
        set_moving_nodes_session(session)
        return list(map(lambda x: x[0],  MovingPoints.get_areas().all()))

    @get("/{area:str}/moving_nodes_by_date")
    def moving_nodes_by_date(self, session: Session, area: str, current_date: int, bounds: str) -> List[Dict]:
        set_moving_nodes_session(session)
        data = MovingPoints.filter_by_date_area(
            datetime.datetime.fromtimestamp(current_date),
            area,
            list(map(lambda x: float(x), bounds.split(",")))
        )
        return list(map(lambda x: dict(x), data.all()))

    @get("/{area:str}/range_dates")
    def range_dates(self, session: Session, area: str) -> Dict:
        set_moving_nodes_session(session)

        data = dict(MovingPoints.get_bounds_by_area(area).all()[0])
        data["data_bounds"] = loads(data["data_bounds"]).bounds
        data["start_date"] = data["start_date"].strftime(date_format)
        data["end_date"] = data["end_date"].strftime(date_format)

        return data


db_routes = Router(
    path="/",
    route_handlers=[FromDbController],
    dependencies={"session": Provide(get_session)}
)
