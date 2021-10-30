from flask import Blueprint
from flask import jsonify
from flask import request
from sqlalchemy.exc import InvalidRequestError

import traceback

from gtfs_builder.gtfs_app.main import GtfsMain


def gtfs_routes(session):

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
            input_data = GtfsMain(session).nodes_by_date(arg_keys["current_date"])

            input_data = jsonify(input_data)
            input_data.headers.add('Access-Control-Allow-Origin', '*')

            return input_data
        except InvalidRequestError:
            # sqlachemy : no further SQL can be emitted within this transaction
            return jsonify(exception=traceback.format_exc()), 204

    return gtfs_routes
