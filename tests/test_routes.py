
def test_range_dates_route(flask_client):
    response = flask_client.get("fake/range_dates", content_type="html/text")
    assert response.status_code == 200

    output_data = response.json
    assert len(output_data["data_bounds"]) == 4
    assert len(output_data["start_date"]) > 0
    assert len(output_data["end_date"]) > 0


def test_nodes_by_date_route(flask_client):
    response = flask_client.get("fake/moving_nodes_by_date?current_date=2007-01-01 09:07:20&bounds=-180,-89,180,89", content_type="html/text")
    assert response.status_code == 200

    output_data = response.json
    assert len(output_data["data_geojson"]) > 0
    assert isinstance(output_data["data_geojson"], list)
    assert set(list(output_data["data_geojson"][0].keys())) == {"stop_code", "x", "y", "route_short_name"}


def test_nodes_by_date_route_invalid(flask_client):
    response = flask_client.get("fake/moving_nodes_by_date?current_date=2008-01-01 09:07:20&bounds=-180,-89,180,89", content_type="html/text")
    assert response.status_code == 200

    output_data = response.json
    assert len(output_data["data_geojson"]) == 0


def test_study_area_list(flask_client):
    response = flask_client.get("fake/existing_study_areas", content_type="html/text")
    assert response.status_code == 200

    output_data = response.json
    assert len(output_data) > 0
