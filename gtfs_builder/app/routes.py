import os
from flask import Blueprint
from flask import jsonify
from flask import request

from gtfs_builder.app.core import GtfsMain


def gtfs_routes(data, study_area_name):

    gtfs_routes = Blueprint(
        'gtfs',
        __name__,
        template_folder='templates',
        url_prefix=f"/api/v1/gtfs/{study_area_name}"
    )

    @gtfs_routes.get("/existing_study_areas")
    def existing_study_areas():
        try:

            input_data = jsonify(os.environ['STUDY_AREA_LIST'].split(","))
            input_data.headers.add('Access-Control-Allow-Origin', '*')

            return input_data

        except Exception as exc:
            return jsonify(exception=exc), 204

    @gtfs_routes.get("/moving_nodes_by_date")
    def moving_nodes_by_date():
        arg_keys = {
            "current_date": request.args.get("current_date", type=str),
            "bounds": request.args.get("bounds", type=str)
        }

        try:
            bounds = map(float, arg_keys["bounds"].split(","))
            input_data = GtfsMain(data).nodes_by_date_from_parquet(arg_keys["current_date"], bounds)

            input_data = jsonify(input_data)
            input_data.headers.add('Access-Control-Allow-Origin', '*')

            return input_data

        except Exception as exc:
            return jsonify(exception=exc), 204

    @gtfs_routes.get("/range_dates")
    def range_dates():

        try:
            input_data = GtfsMain(data).context_data_from_parquet()

            input_data = jsonify(input_data)
            input_data.headers.add('Access-Control-Allow-Origin', '*')

            return input_data
        except Exception as exc:
            return jsonify(exception=exc), 204

    return gtfs_routes