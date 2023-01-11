import pytest

import os
from starlite import TestClient

from gtfs_builder.app.config import settings
from gtfs_builder.app.session import get_session


@pytest.fixture
def credentials():
    return {
        "study_area_name": "fake",
        "input_data_dir": "tests/fixture/gtfs",
        "output_data_dir": os.path.join(os.getcwd(), settings.DATA_DIR),
        "transport_modes": ["bus", "metro"],
        "date_mode": "calendar",
        "date": "20070604",
        "interpolation_threshold": 1000
    }


@pytest.fixture
def session_db():

    return get_session()


@pytest.fixture(scope="module")
def client_from_file():
    settings.MODE = "file"
    from app import app
    with TestClient(app) as client:
        yield client


@pytest.fixture(scope="module")
def client_from_db():
    settings.MODE = "db"
    from app import app
    with TestClient(app) as client:
        yield client


@pytest.fixture
def url_prefix():
    return f"{settings.API_PREFIX}/"
