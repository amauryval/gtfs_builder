from typing import List, Dict

from gtfs_builder.app.global_values import api_cors_config, date_format
from gtfs_builder.app.global_values import existing_study_areas_route_name
from gtfs_builder.app.global_values import range_dates_route_name
from gtfs_builder.app.global_values import moving_nodes_by_date_route_name
from gtfs_builder.app.global_values import blueprint_name
from gtfs_builder.app.global_values import url_prefix

from gtfs_builder.app.helpers import RouteBuilder

from gtfs_builder.db import MovingPoints

from shapely.wkt import loads

import datetime


def gtfs_routes_from_db(session):

    gtfs_routes = Blueprint(
        blueprint_name,
        __name__,
        template_folder='templates',
        url_prefix=url_prefix
    )
    MovingPoints.set_session(session)

    @gtfs_routes.get(existing_study_areas_route_name)
    @cross_origin(origins=api_cors_config)
    @RouteBuilder()
    def existing_study_areas() -> List[str]:

        return list(map(lambda x: x[0],  MovingPoints.get_areas().all()))

    @gtfs_routes.get(moving_nodes_by_date_route_name)
    @cross_origin(origins=api_cors_config)
    @RouteBuilder(expected_args={"current_date", "bounds"})
    def moving_nodes_by_date(area: str) -> List[Dict]:
        arg_keys = {
            "current_date": datetime.datetime.strptime(request.args.get("current_date", type=str), date_format),
            "bounds": request.args.get("bounds", type=str).split(',')
        }

        data = MovingPoints.filter_by_date_area(arg_keys["current_date"], area, arg_keys["bounds"])
        return list(map(lambda x: dict(x), data.all()))

    @gtfs_routes.get(range_dates_route_name)
    @cross_origin(origins=api_cors_config)
    @RouteBuilder()
    def range_dates(area: str) -> Dict:

        data = dict(MovingPoints.get_bounds_by_area(area).all()[0])
        data["data_bounds"] = loads(data["data_bounds"]).bounds
        data["start_date"] = data["start_date"].strftime(date_format)
        data["end_date"] = data["end_date"].strftime(date_format)

        return data

    return gtfs_routes
