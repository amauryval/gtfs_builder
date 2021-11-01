
def test_range_dates_route(flask_client):
    response = flask_client.get("/range_dates", content_type="html/text")
    assert response.status_code == 200

    output_data = response.json
    assert len(output_data["data_bounds"]) == 4
    assert len(output_data["start_date"]) > 0
    assert len(output_data["end_date"]) > 0


def test_nodes_by_date_route(flask_client):
    response = flask_client.get("/nodes_by_date?current_date=2019-11-15 10:01:00", content_type="html/text")
    assert response.status_code == 200

    output_data = response.json
    assert len(output_data["data_geojson"]) > 0
    assert isinstance(output_data["data_geojson"], list)
    assert set(list(output_data["data_geojson"][0].keys())) == set(["stop_code", "x", "y", "line_name_short"])

