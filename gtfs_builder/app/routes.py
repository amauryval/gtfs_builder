import os
from flask import Blueprint
from flask import jsonify
from flask import request

from gtfs_builder.app.core import GtfsMain

import psycopg2
import re

def str_to_dict_from_regex(string_value, regex):
    pattern = re.compile(regex)
    extraction = pattern.match(string_value)
    return extraction.groupdict()


def gtfs_routes():

    gtfs_routes = Blueprint(
        f"gtfs",
        __name__,
        template_folder='templates',
        url_prefix=f"/api/v1/gtfs_builder/"
    )

    _credentials = {
        **str_to_dict_from_regex(
            os.environ.get("ADMIN_DB_URL"),
            ".+:\/\/(?P<user>.+):(?P<password>.+)@(?P<host>[\W\w-]+):(?P<port>\d+)\/(?P<database>.+)"
        ),
    }
    conn = psycopg2.connect(**_credentials)

    @gtfs_routes.get("<area>/existing_study_areas")
    def existing_study_areas(area):

        try:

            input_data = jsonify(area)
            input_data.headers.add('Access-Control-Allow-Origin', '*')

            return input_data

        except Exception as exc:
            return jsonify(exception=exc), 204

    @gtfs_routes.get("<area>/moving_nodes_by_date")
    def moving_nodes_by_date(area):
        arg_keys = {
            "current_date": request.args.get("current_date", type=str),
            "bounds": request.args.get("bounds", type=str)
        }
        try:
            bounds = map(float, arg_keys["bounds"].split(","))
            input_data = GtfsMain(conn, area).nodes_by_date_from_db(arg_keys["current_date"], bounds)

            input_data = jsonify(input_data)
            input_data.headers.add('Access-Control-Allow-Origin', '*')

            return input_data

        except Exception as exc:
            return jsonify(exception=exc), 204

    @gtfs_routes.get("<area>/range_dates")
    def range_dates(area):
        try:
            # input_data = GtfsMain(data_from_area["data"]).context_data_from_parquet()
            input_data = GtfsMain(conn, area).context_data_from_db()

            input_data = jsonify(input_data)
            input_data.headers.add('Access-Control-Allow-Origin', '*')

            return input_data
        except Exception as exc:
            return jsonify(exception=exc), 204

    return gtfs_routes