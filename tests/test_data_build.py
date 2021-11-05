

from gtfs_builder.main import GtfsFormater

import spatialpandas.io as sp


def test_data_processing_full_data(credentials):
    GtfsFormater(
        credentials["study_area_name"],
        credentials["path_data"],
        credentials["transport_modes"],
        credentials["days"],
    )
    base_lines = sp.read_parquet("fake_base_lines_data.parq")
    assert base_lines.shape == (11, 8)
    assert base_lines.columns.tolist() == ['shape_id', 'geometry', 'route_desc', 'route_type', 'route_short_name', 'direction_id', 'route_color', 'route_text_color']

    base_stops = sp.read_parquet("fake_base_stops_data.parq")
    assert base_stops.shape == (1, 8)
    assert base_stops.columns.tolist() == ['stop_code', 'geometry', 'stop_name', 'route_short_name', 'route_desc', 'route_type', 'route_color', 'route_text_color']

    moving_stops = sp.read_parquet("fake_moving_stops.parq")
    assert moving_stops.shape == (137, 14)
    assert moving_stops.columns.tolist() == ['start_date', 'end_date', 'stop_code', 'x', 'y', 'geometry', 'stop_name', 'pos', 'route_type', 'route_long_name', 'route_short_name', 'direction_id', 'shape_id', 'trip_id']


def test_data_processing_with_shape_id_computed(credentials):
    GtfsFormater(
        credentials["study_area_name"],
        credentials["path_data"],
        credentials["transport_modes"],
        credentials["days"],
        True
    )
    base_lines = sp.read_parquet("fake_base_lines_data.parq")
    assert base_lines.shape == (9, 8)
    assert base_lines.columns.tolist() == ['shape_id', 'geometry', 'route_desc', 'route_type', 'route_short_name', 'direction_id', 'route_color', 'route_text_color']

    base_stops = sp.read_parquet("fake_base_stops_data.parq")
    assert base_stops.shape == (1, 8)
    assert base_stops.columns.tolist() == ['stop_code', 'geometry', 'stop_name', 'route_short_name', 'route_desc', 'route_type', 'route_color', 'route_text_color']

    moving_stops = sp.read_parquet("fake_moving_stops.parq")
    assert moving_stops.shape == (137, 14)
    assert moving_stops.columns.tolist() == ['start_date', 'end_date', 'stop_code', 'x', 'y', 'geometry', 'stop_name', 'pos', 'route_type', 'route_long_name', 'route_short_name', 'direction_id', 'shape_id', 'trip_id']
