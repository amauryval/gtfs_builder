import pytest

from gtfs_builder.app.routes_from_db import gtfs_routes_from_db
from gtfs_builder.app.routes_from_files import gtfs_routes_from_files

from flask import Flask

import os

import spatialpandas.io as sp_io

from gtfs_builder.main import str_to_dict_from_regex
from gtfs_builder.db.base import Base

from dotenv import load_dotenv
from geospatial_lib import GeoSpatialLib


def get_db_session():
    credentials = {
        **str_to_dict_from_regex(
            os.environ.get("ADMIN_DB_URL"),
            r".+:\/\/(?P<username>.+):(?P<password>.+)@(?P<host>.+):(?P<port>\d{4})\/(?P<database>.+)"
        ),
        **{"scoped_session": True}
    }

    session, _ = GeoSpatialLib().sqlalchemy_connection(**credentials)
    return session


@pytest.fixture
def credentials():
    return {
        "study_area_name": "fake",
        "input_data_dir": "tests/fixture/gtfs",
        "transport_modes": ["bus", "metro"],
        "date_mode": "calendar",
        "date": "20070604",
        "interpolation_threshold": 1000
  }


@pytest.fixture
def session_db():
    load_dotenv()
    return get_db_session()


def pytest_sessionfinish(session):
    # remove file
    files_to_remove = ["fake_base_lines_data.parq", "fake_base_stops_data.parq", "fake_moving_stops.parq"]
    for input_file in files_to_remove:
        if os.path.isfile(input_file):
            print(f"File {input_file} removed")

            os.remove(input_file)

    db_session = get_db_session()

    # clean db
    Base.metadata.drop_all(db_session.bind)


@pytest.fixture(scope="module")
def flask_client_from_file():

    areas_list = ["fake"]

    data = {
        study_area: {
            "data": sp_io.read_parquet(f"{study_area.lower()}_moving_stops.parq"),
            "study_area": study_area
        }
        for study_area in areas_list
    }

    app = Flask(__name__)
    app.register_blueprint(gtfs_routes_from_files(data, areas_list=areas_list), url_prefix="")
    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
    app.config['JSON_SORT_KEYS'] = False
    app.config['DEBUG'] = True
    app.config['TESTING'] = True
    gtfs_api_client = app.test_client()

    yield gtfs_api_client


@pytest.fixture(scope="module")
def flask_client_from_db():

    app = Flask(__name__)
    app.register_blueprint(gtfs_routes_from_db(get_db_session()), url_prefix="")
    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
    app.config['JSON_SORT_KEYS'] = False
    app.config['DEBUG'] = True
    app.config['TESTING'] = True
    gtfs_api_client = app.test_client()

    yield gtfs_api_client