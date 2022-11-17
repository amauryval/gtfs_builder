from typing import List, Dict

from flask_cors import cross_origin

from flask import Blueprint
from flask import request

from gtfs_builder.app.global_values import api_cors_config
from gtfs_builder.app.global_values import url_prefix
from gtfs_builder.app.global_values import blueprint_name
from gtfs_builder.app.global_values import existing_study_areas_route_name
from gtfs_builder.app.global_values import range_dates_route_name
from gtfs_builder.app.global_values import moving_nodes_by_date_route_name
from gtfs_builder.app.global_values import route_types_route_name

from gtfs_builder.app.core import GtfsMain

from gtfs_builder.app.helpers import RouteBuilder


def gtfs_routes_from_files(data, areas_list):

    gtfs_routes = Blueprint(
        blueprint_name,
        __name__,
        template_folder='templates',
        url_prefix=url_prefix
    )

    @gtfs_routes.get(existing_study_areas_route_name)
    @cross_origin(origins=api_cors_config)
    @RouteBuilder()
    def existing_study_areas() -> List[str]:

        return areas_list

    @gtfs_routes.get(moving_nodes_by_date_route_name)
    @cross_origin(origins=api_cors_config)
    @RouteBuilder(expected_args={"current_date", "bounds"}, optional_args={"route_type"})
    def moving_nodes_by_date(area: str) -> List[Dict]:

        arg_keys = {
            "current_date": request.args.get("current_date", type=str),
            "bounds": request.args.get("bounds", type=str),
            "route_type": request.args.get("route_type", default=None)  # optional url arg

        }
        data_from_area = data[area]
        bounds = list(map(float, arg_keys["bounds"].split(",")))
        return GtfsMain(
            data_from_area["data"]).nodes_by_date_from_parquet(
            arg_keys["current_date"], bounds, arg_keys["route_type"]
        )

    @gtfs_routes.get(range_dates_route_name)
    @cross_origin(origins=api_cors_config)
    @RouteBuilder()
    def range_dates(area: str) -> Dict:

        data_from_area = data[area]
        return GtfsMain(data_from_area["data"]).context_data_from_parquet()

    @gtfs_routes.get(route_types_route_name)
    @cross_origin(origins=api_cors_config)
    @RouteBuilder()
    def transport_types(area: str) -> List[str]:

        data_from_area = data[area]
        return GtfsMain(data_from_area["data"]).transport_types_from_parquet()

    return gtfs_routes
