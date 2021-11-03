import pytest

from gtfs_builder.gtfs_app.routes import gtfs_routes

from flask import Flask
from dotenv import load_dotenv

from spatialpandas import io

from fixture.credentials import *

load_dotenv(".gtfs.env")


@pytest.fixture(scope="session")
def flask_client():

    session = io.read_parquet("fake_moving_stops.parq")

    app = Flask(__name__)
    app.register_blueprint(gtfs_routes(session), url_prefix="")
    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
    app.config['JSON_SORT_KEYS'] = False
    app.config['DEBUG'] = True
    app.config['TESTING'] = True

    gtfs_api_client = app.test_client()

    yield gtfs_api_client
