from flask import Blueprint
from flask import jsonify
from flask import request

import traceback

from gtfs_builder.gtfs_app.core import GtfsMain

from gtfs_builder.gtfs_db.stops import StopsGeom
from gtfs_builder.gtfs_db.stops_times import StopsTimesValues
import os


def gtfs_routes(session):

    gtfs_routes = Blueprint(
        'gtfs',
        __name__,
        template_folder='templates',
        url_prefix='/api/v1/gtfs'
    )
    StopsGeom.set_session(session)
    StopsTimesValues.set_session(session)

    @gtfs_routes.get("/nodes_by_date")
    def nodes_by_date():
        arg_keys = {
            "current_date": request.args.get("current_date", type=str),
        }

        try:

            input_data = GtfsMain(session).nodes_by_date_from_parquet(arg_keys["current_date"])

            input_data = jsonify(input_data)
            input_data.headers.add('Access-Control-Allow-Origin', '*')

            return input_data

        except Exception as exc:
            return jsonify(exception=exc), 204

    @gtfs_routes.get("/range_dates")
    def range_dates():

        try:
            input_data = GtfsMain(session).context_data_from_parquet()

            input_data = jsonify(input_data)
            input_data.headers.add('Access-Control-Allow-Origin', '*')

            return input_data
        except Exception as exc:
            return jsonify(exception=exc), 204

    return gtfs_routes