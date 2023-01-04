
def test_range_dates_route(client_from_file, url_prefix):
    response = client_from_file.get(url_prefix + "fake/range_dates")
    assert response.status_code == 200

    output_data = response.json()
    assert len(output_data["data_bounds"]) == 4
    assert len(output_data["start_date"]) > 0
    assert len(output_data["end_date"]) > 0


def test_nodes_by_date_route_with_route_type(client_from_file, url_prefix):
    response = client_from_file.get(url_prefix + "fake/moving_nodes_by_date?current_date=1167642440&bounds=-180,-89,180,89&route_type=3")
    assert response.status_code == 200, response.content

    output_data = response.json()
    assert len(output_data) == 1
    assert isinstance(output_data, list)
    assert set(list(output_data[0].keys())) == {'route_long_name', 'x', 'route_type', 'y'}


def test_nodes_by_date_route_without_route_type(client_from_file, url_prefix):
    response = client_from_file.get(url_prefix + "fake/moving_nodes_by_date?"
                                                 "current_date=1167642440&bounds=-180,-89,180,89")

    assert response.status_code == 200, response.content

    output_data = response.json()
    assert len(output_data) > 0
    assert isinstance(output_data, list)
    assert set(list(output_data[0].keys())) == {'route_long_name', 'x', 'route_type', 'y'}


def test_nodes_by_date_route_invalid(client_from_file, url_prefix):
    response = client_from_file.get(url_prefix + "fake/moving_nodes_by_date?"
                                                 "current_date=1199178440&bounds=-180,-89,180,89")
    assert response.status_code == 200

    output_data = response.json()
    assert len(output_data) == 0


def test_study_area_list(client_from_file, url_prefix):
    response = client_from_file.get(url_prefix + "existing_study_areas")
    assert response.status_code == 200

    output_data = response.json()
    assert len(output_data) > 0
