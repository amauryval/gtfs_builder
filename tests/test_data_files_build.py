import os
from gtfs_builder.main import GtfsRebuilder

import spatialpandas.io as sp_io

def remove_output_file():
    files_to_remove = ["fake_base_lines_data.parq", "fake_base_stops_data.parq", "_fake_moving_stops.parq"]

    for input_file in files_to_remove:
        if os.path.isfile(input_file):
            os.remove(input_file)


def test_data_processing_full_data_thresh_2(credentials):
    remove_output_file()
    GtfsRebuilder(
        study_area_name=credentials["study_area_name"],
        data_path=credentials["input_data_dir"],
        transport_modes=credentials["transport_modes"],
        date_mode=credentials["date_mode"],
        date=credentials["date"],
        build_shape_data=False,
        interpolation_threshold=500,
        output_format="file"
    )
    base_lines = sp_io.read_parquet("fake_base_lines_data.parq")
    assert base_lines.shape == (7, 8)
    assert set(base_lines.columns.tolist()) == {'shape_id', 'geometry', 'route_desc', 'route_type', 'route_short_name',
                                                'direction_id', 'route_color', 'route_text_color'}

    base_stops = sp_io.read_parquet("fake_base_stops_data.parq")
    assert base_stops.shape == (8, 8)
    assert set(base_stops.columns.tolist()) == {'stop_code', 'geometry', 'stop_name', 'route_short_name', 'route_desc',
                                                'route_type', 'route_color', 'route_text_color'}

    moving_stops = sp_io.read_parquet("fake_moving_stops.parq")
    assert moving_stops.shape == (407, 10)
    assert set(moving_stops.columns.tolist()) == {'start_date', 'end_date', "geometry", 'stop_code', 'x', 'y',
                                                  'stop_name', 'route_type', 'route_long_name', 'route_short_name'}


def test_data_processing_full_data_calendar_dates(credentials):
    remove_output_file()
    GtfsRebuilder(
        study_area_name=credentials["study_area_name"],
        data_path=credentials["input_data_dir"],
        transport_modes=credentials["transport_modes"],
        date_mode="calendar_dates",
        date=credentials["date"],
        build_shape_data=False,
        interpolation_threshold=500,
        output_format="file"
    )
    base_lines = sp_io.read_parquet("fake_base_lines_data.parq")
    assert base_lines.shape == (7, 8)
    assert set(base_lines.columns.tolist()) == {'shape_id', 'geometry', 'route_desc', 'route_type', 'route_short_name',
                                                'direction_id', 'route_color', 'route_text_color'}

    base_stops = sp_io.read_parquet("fake_base_stops_data.parq")
    assert base_stops.shape == (8, 8)
    assert set(base_stops.columns.tolist()) == {'stop_code', 'geometry', 'stop_name', 'route_short_name', 'route_desc',
                                                'route_type', 'route_color', 'route_text_color'}

    moving_stops = sp_io.read_parquet("fake_moving_stops.parq")
    assert moving_stops.shape == (407, 10)
    assert set(moving_stops.columns.tolist()) == {'start_date', 'end_date', "geometry", 'stop_code', 'x', 'y',
                                                  'stop_name', 'route_type', 'route_long_name', 'route_short_name'}


def test_data_processing_with_shape_id_computed(credentials):
    remove_output_file()

    GtfsRebuilder(
        study_area_name=credentials["study_area_name"],
        data_path=credentials["input_data_dir"],
        transport_modes=credentials["transport_modes"],
        date_mode=credentials["date_mode"],
        date=credentials["date"],
        build_shape_data=True,
        interpolation_threshold=200,
        output_format="file"
    )
    base_lines = sp_io.read_parquet("fake_base_lines_data.parq")
    assert base_lines.shape == (7, 8)
    assert set(base_lines.columns.tolist()) == {'shape_id', 'geometry', 'route_desc', 'route_type', 'route_short_name',
                                                'direction_id', 'route_color', 'route_text_color'}

    base_stops = sp_io.read_parquet("fake_base_stops_data.parq")
    assert base_stops.shape == (8, 8)
    assert set(base_stops.columns.tolist()) == {'stop_code', 'geometry', 'stop_name', 'route_short_name', 'route_desc',
                                                'route_type', 'route_color', 'route_text_color'}

    moving_stops = sp_io.read_parquet("fake_moving_stops.parq")
    assert moving_stops.shape == (735, 10)
    assert set(moving_stops.columns.tolist()) == {'start_date', 'end_date', "geometry", 'stop_code', 'x', 'y',
                                                  'stop_name', 'route_type', 'route_long_name', 'route_short_name'}


def test_data_processing_full_data_tresh_1(credentials):
    # Warning let this test at the end of the file
    remove_output_file()
    GtfsRebuilder(
        study_area_name=credentials["study_area_name"],
        data_path=credentials["input_data_dir"],
        transport_modes=credentials["transport_modes"],
        date_mode=credentials["date_mode"],
        date="20070603",
        build_shape_data=False,
        interpolation_threshold=1000,
        output_format="file"
    )
    base_lines = sp_io.read_parquet("fake_base_lines_data.parq")
    assert base_lines.shape == (9, 8)
    assert set(base_lines.columns.tolist()) == {'shape_id', 'geometry', 'route_desc', 'route_type', 'route_short_name',
                                                'direction_id', 'route_color', 'route_text_color'}

    base_stops = sp_io.read_parquet("fake_base_stops_data.parq")
    assert base_stops.shape == (9, 8)
    assert set(base_stops.columns.tolist()) == {'stop_code', 'geometry', 'stop_name', 'route_short_name', 'route_desc',
                                                'route_type', 'route_color', 'route_text_color'}

    moving_stops = sp_io.read_parquet("fake_moving_stops.parq")
    assert moving_stops.shape == (256, 10)
    assert set(moving_stops.columns.tolist()) == {'start_date', 'end_date', "geometry", 'stop_code', 'x', 'y',
                                                  'stop_name', 'route_type', 'route_long_name', 'route_short_name'}