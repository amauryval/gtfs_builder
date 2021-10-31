from flask import Blueprint
from flask import jsonify
from flask import request
from sqlalchemy.exc import InvalidRequestError

import traceback

from gtfs_builder.gtfs_app.main import GtfsMain


def gtfs_routes(session, mode_from):

    gtfs_routes = Blueprint(
        'gtfs',
        __name__,
        template_folder='templates',
        url_prefix='/api/v1/gtfs'
    )

    @gtfs_routes.get("/nodes_by_date")
    def nodes_by_date():

        arg_keys = {
            "current_date": request.args.get("current_date", type=str),
        }

        try:
            if mode_from == "db":
                input_data = GtfsMain().nodes_by_date_from_db(session, arg_keys["current_date"])
            elif mode_from == "parquet":
                input_data = GtfsMain().nodes_by_date_from_parquet(session, arg_keys["current_date"])

            input_data = jsonify(input_data)
            input_data.headers.add('Access-Control-Allow-Origin', '*')

            return input_data
        except InvalidRequestError:
            # sqlachemy : no further SQL can be emitted within this transaction
            return jsonify(exception=traceback.format_exc()), 204

    @gtfs_routes.get("/range_dates")
    def range_dates():

        try:
            if mode_from == "db":
                input_data = GtfsMain().dates_range_from_db(session)
            elif mode_from == "parquet":
                input_data = GtfsMain().dates_range_from_parquet(session)

            input_data = jsonify(input_data)
            input_data.headers.add('Access-Control-Allow-Origin', '*')

            return input_data
        except InvalidRequestError:
            # sqlachemy : no further SQL can be emitted within this transaction
            return jsonify(exception=traceback.format_exc()), 204

    return gtfs_routes