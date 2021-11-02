import pytest


@pytest.fixture
def credentials():
    return {
        "study_area_name": "fake",
        "path_data": "fixture/gtfs",
        "transport_modes": ["bus"],
        "days": ["friday"],
    }