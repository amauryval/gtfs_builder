import pytest

from gtfs_builder.app.routes import gtfs_routes

from flask import Flask

import spatialpandas.io as sp_io

from fixture.credentials import *


@pytest.fixture(scope="session")
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
