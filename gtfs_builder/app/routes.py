import os
from flask import Blueprint
from flask import jsonify
from flask import request

from gtfs_builder.app.core import GtfsMain

import psycopg2
import re
import sqlite3

from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker

from sqlalchemy.event import listen
from sqlalchemy.orm import scoped_session


def load_spatialite(dbapi_conn, connection_record):
    dbapi_conn.enable_load_extension(True)
    dbapi_conn.load_extension("mod_spatialite")
    dbapi_conn.enable_load_extension(False)


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

    _credentials = os.environ.get("ADMIN_DB_URL")
    os.environ['PATH'] = os.environ['SPATALITE_PATH'] + ';' + os.environ['PATH']

    # _engine = create_engine(
    #     f"sqlite:///{_credentials}?check_same_thread=False",
    #     # pool_size=50,     # default in SQLAlchemy
    #     # max_overflow=50, # default in SQLAlchemy
    #     # pool_timeout=1,  # raise an error faster than default
    # )
    from sqlalchemy.pool import StaticPool
    _engine = create_engine(f"sqlite:///{_credentials}",connect_args={'check_same_thread': False}, poolclass=StaticPool)
    listen(_engine, 'connect', load_spatialite)
    conn = sessionmaker(bind=_engine)()
    # conn = scoped_session(session_factory)()

    # conn = sqlite3.connect(f"{_credentials}")

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