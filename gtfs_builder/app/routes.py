from flask_cors import cross_origin
import traceback

import functools

from flask import Blueprint
from flask import jsonify
from flask import request

from gtfs_builder.app.core import GtfsMain

api_cors_config = [
    "http://localhost:4200",
    "https://portfolio.amaury-valorge.com"
]


class RouteBuilder:
    __slots__ = ()

    def __init__(self):
        pass

    def __call__(self, func):

        @functools.wraps(func)
        def wrapper_route(*args, **kwargs):
            try:

                return jsonify(func(*args, **kwargs))

            except (Exception,):

                return jsonify(exception=traceback.format_exc()), 400

        return wrapper_route


def gtfs_routes(data, areas_list):

    gtfs_routes = Blueprint(
        f"gtfs",
        __name__,
        template_folder='templates',
        url_prefix=f"/api/v1/gtfs_builder/"
    )

    @gtfs_routes.get("/existing_study_areas")
    @cross_origin(origins=api_cors_config)
    @RouteBuilder()
    def existing_study_areas():
        assert request.args.keys() == set([])

        return areas_list

    @gtfs_routes.get("<area>/moving_nodes_by_date")
    @cross_origin(origins=api_cors_config)
    @RouteBuilder()
    def moving_nodes_by_date(area):
        assert request.args.keys() == {"current_date", "bounds"}

        arg_keys = {
            "current_date": request.args.get("current_date", type=str),
            "bounds": request.args.get("bounds", type=str)
        }
        data_from_area = data[area]
        bounds = map(float, arg_keys["bounds"].split(","))
        return GtfsMain(data_from_area["data"]).nodes_by_date_from_parquet(arg_keys["current_date"], bounds)

    @gtfs_routes.get("<area>/range_dates")
    @cross_origin(origins=api_cors_config)
    @RouteBuilder()
    def range_dates(area):
        assert request.args.keys() == set([])

        data_from_area = data[area]
        return GtfsMain(data_from_area["data"]).context_data_from_parquet()

    return gtfs_routes
