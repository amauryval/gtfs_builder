import pytest


@pytest.fixture
def credentials():
    return {
        "study_area_name": "fake",
        "input_data_dir": "tests/fixture/gtfs",
        "transport_modes": ["bus"],
        "date_mode": "calendar",
        "date": "20070604",
        "interpolation_threshold": 1000
  }