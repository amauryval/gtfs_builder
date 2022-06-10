import pytest

from gtfs_builder.app.routes import gtfs_routes

from flask import Flask

import os

import spatialpandas.io as sp_io

from gtfs_builder.main import GtfsFormater


def pytest_sessionstart(session):
    credentials = {
        "study_area_name": "fake",
        "input_data_dir":  os.path.join(os.getcwd(), r"tests\fixture\gtfs"),
        "transport_modes": ["bus"],
        "date_mode": "calendar",
        "date": "20070604",
        "interpolation_threshold": 1000
    }

    GtfsFormater(
        study_area_name=credentials["study_area_name"],
        data_path=credentials["input_data_dir"],
        transport_modes=credentials["transport_modes"],
        date_mode="calendar_dates",
        date=credentials["date"],
        build_shape_data=False,
        interpolation_threshold=500
    )


def pytest_sessionfinish(session):
    files_to_remove = ["fake_base_lines_data.parq", "fake_base_stops_data.parq", "fake_moving_stops.parq"]

    for input_file in files_to_remove:
        if os.path.isfile(input_file):
            os.remove(input_file)


@pytest.fixture(scope="module")
def flask_client():

    areas_list = ["fake"]

    data = {
        study_area: {
            "data": sp_io.read_parquet(f"{study_area}_moving_stops.parq"),
            "study_area": study_area
        }
        for study_area in areas_list
    }

    app = Flask(__name__)
    app.register_blueprint(gtfs_routes(data, areas_list=areas_list), url_prefix="")
    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
    app.config['JSON_SORT_KEYS'] = False
    app.config['DEBUG'] = True
    app.config['TESTING'] = True
    gtfs_api_client = app.test_client()

    yield gtfs_api_client
