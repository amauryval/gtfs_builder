import os

from gtfs_builder.main import GtfsFormater

import spatialpandas.io as sp


def remove_output_file():
    files_to_remove = ["fake_base_lines_data.parq", "fake_base_stops_data.parq", "fake_moving_stops.parq"]

    for input_file in files_to_remove:
        if os.path.isfile(input_file):
            os.remove(input_file)


def test_data_processing_full_data_thresh_2(credentials):
    remove_output_file()
    GtfsFormater(
        study_area_name=credentials["study_area_name"],
        data_path=credentials["input_data_dir"],
        transport_modes=credentials["transport_modes"],
        date_mode=credentials["date_mode"],
        date=credentials["date"],
        build_shape_data=False,
        interpolation_threshold=500
    )
    base_lines = sp.read_parquet("fake_base_lines_data.parq")
    assert base_lines.shape == (9, 8)
    assert base_lines.columns.tolist() == ['shape_id', 'geometry', 'route_desc', 'route_type', 'route_short_name', 'direction_id', 'route_color', 'route_text_color']

    base_stops = sp.read_parquet("fake_base_stops_data.parq")
    assert base_stops.shape == (8, 8)
    assert base_stops.columns.tolist() == ['stop_code', 'geometry', 'stop_name', 'route_short_name', 'route_desc', 'route_type', 'route_color', 'route_text_color']

    moving_stops = sp.read_parquet("fake_moving_stops.parq")
    assert moving_stops.shape == (423, 14)
    assert moving_stops.columns.tolist() == ['start_date', 'end_date', 'stop_code', 'x', 'y', 'geometry', 'stop_name', 'pos', 'route_type', 'route_long_name', 'route_short_name', 'direction_id', 'shape_id', 'trip_id']


def test_data_processing_full_data_calendar_dates(credentials):
    remove_output_file()
    GtfsFormater(
        study_area_name=credentials["study_area_name"],
        data_path=credentials["input_data_dir"],
        transport_modes=credentials["transport_modes"],
        date_mode="calendar_dates",
        date=credentials["date"],
        build_shape_data=False,
        interpolation_threshold=500
    )
    base_lines = sp.read_parquet("fake_base_lines_data.parq")
    assert base_lines.shape == (9, 8)
    assert base_lines.columns.tolist() == ['shape_id', 'geometry', 'route_desc', 'route_type', 'route_short_name', 'direction_id', 'route_color', 'route_text_color']

    base_stops = sp.read_parquet("fake_base_stops_data.parq")
    assert base_stops.shape == (8, 8)
    assert base_stops.columns.tolist() == ['stop_code', 'geometry', 'stop_name', 'route_short_name', 'route_desc', 'route_type', 'route_color', 'route_text_color']

    moving_stops = sp.read_parquet("fake_moving_stops.parq")
    assert moving_stops.shape == (423, 14)
    assert moving_stops.columns.tolist() == ['start_date', 'end_date', 'stop_code', 'x', 'y', 'geometry', 'stop_name', 'pos', 'route_type', 'route_long_name', 'route_short_name', 'direction_id', 'shape_id', 'trip_id']


def test_data_processing_with_shape_id_computed(credentials):
    remove_output_file()

    GtfsFormater(
        study_area_name=credentials["study_area_name"],
        data_path=credentials["input_data_dir"],
        transport_modes=credentials["transport_modes"],
        date_mode=credentials["date_mode"],
        date=credentials["date"],
        build_shape_data=True,
        interpolation_threshold=200
    )
    base_lines = sp.read_parquet("fake_base_lines_data.parq")
    assert base_lines.shape == (9, 8)
    assert base_lines.columns.tolist() == ['shape_id', 'geometry', 'route_desc', 'route_type', 'route_short_name', 'direction_id', 'route_color', 'route_text_color']

    base_stops = sp.read_parquet("fake_base_stops_data.parq")
    assert base_stops.shape == (8, 8)
    assert base_stops.columns.tolist() == ['stop_code', 'geometry', 'stop_name', 'route_short_name', 'route_desc', 'route_type', 'route_color', 'route_text_color']

    moving_stops = sp.read_parquet("fake_moving_stops.parq")
    assert moving_stops.shape == (370, 14)
    assert moving_stops.columns.tolist() == ['start_date', 'end_date', 'stop_code', 'x', 'y', 'geometry', 'stop_name', 'pos', 'route_type', 'route_long_name', 'route_short_name', 'direction_id', 'shape_id', 'trip_id']


def test_data_processing_full_data_tresh_1(credentials):
    # Warning let this test at the end of the file
    remove_output_file()
    GtfsFormater(
        study_area_name=credentials["study_area_name"],
        data_path=credentials["input_data_dir"],
        transport_modes=credentials["transport_modes"],
        date_mode=credentials["date_mode"],
        date=credentials["date"],
        build_shape_data=False,
        interpolation_threshold=1000
    )
    base_lines = sp.read_parquet("fake_base_lines_data.parq")
    assert base_lines.shape == (9, 8)
    assert base_lines.columns.tolist() == ['shape_id', 'geometry', 'route_desc', 'route_type', 'route_short_name', 'direction_id', 'route_color', 'route_text_color']

    base_stops = sp.read_parquet("fake_base_stops_data.parq")
    assert base_stops.shape == (8, 8)
    assert base_stops.columns.tolist() == ['stop_code', 'geometry', 'stop_name', 'route_short_name', 'route_desc', 'route_type', 'route_color', 'route_text_color']

    moving_stops = sp.read_parquet("fake_moving_stops.parq")
    assert moving_stops.shape == (215, 14)
    assert moving_stops.columns.tolist() == ['start_date', 'end_date', 'stop_code', 'x', 'y', 'geometry', 'stop_name', 'pos', 'route_type', 'route_long_name', 'route_short_name', 'direction_id', 'shape_id', 'trip_id']
