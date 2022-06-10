
import spatialpandas.io as sp_io

def test_data_processing_full_data_thresh_2():
    base_lines = sp_io.read_parquet("fake_base_lines_data.parq")
    assert base_lines.shape == (7, 8)
    assert set(base_lines.columns.tolist()) == {'shape_id', 'geometry', 'route_desc', 'route_type', 'route_short_name',
                                                'direction_id', 'route_color', 'route_text_color'}

    base_stops = sp_io.read_parquet("fake_base_stops_data.parq")
    assert base_stops.shape == (8, 8)
    assert set(base_stops.columns.tolist()) == {'stop_code', 'geometry', 'stop_name', 'route_short_name', 'route_desc',
                                                'route_type', 'route_color', 'route_text_color'}

    moving_stops = sp_io.read_parquet("fake_moving_stops.parq")
    assert moving_stops.shape == (436, 11)
    assert set(moving_stops.columns.tolist()) == {'start_date', 'end_date', "geometry", 'stop_code', 'x', 'y',
                                                  'stop_name', 'route_type', 'route_long_name', 'route_short_name',
                                                  'direction_id'}


def test_data_processing_full_data_calendar_dates():
    base_lines = sp_io.read_parquet("fake_base_lines_data.parq")
    assert base_lines.shape == (7, 8)
    assert set(base_lines.columns.tolist()) == {'shape_id', 'geometry', 'route_desc', 'route_type', 'route_short_name',
                                                'direction_id', 'route_color', 'route_text_color'}

    base_stops = sp_io.read_parquet("fake_base_stops_data.parq")
    assert base_stops.shape == (8, 8)
    assert set(base_stops.columns.tolist()) == {'stop_code', 'geometry', 'stop_name', 'route_short_name', 'route_desc',
                                                'route_type', 'route_color', 'route_text_color'}

    moving_stops = sp_io.read_parquet("fake_moving_stops.parq")
    assert moving_stops.shape == (436, 11)
    assert set(moving_stops.columns.tolist()) == {'start_date', 'end_date', "geometry", 'stop_code', 'x', 'y',
                                                  'stop_name', 'route_type', 'route_long_name', 'route_short_name',
                                                  'direction_id'}


def test_data_processing_with_shape_id_computed():
    base_lines = sp_io.read_parquet("fake_base_lines_data.parq")
    assert base_lines.shape == (7, 8)
    assert set(base_lines.columns.tolist()) == {'shape_id', 'geometry', 'route_desc', 'route_type', 'route_short_name',
                                                'direction_id', 'route_color', 'route_text_color'}

    base_stops = sp_io.read_parquet("fake_base_stops_data.parq")
    assert base_stops.shape == (8, 8)
    assert set(base_stops.columns.tolist()) == {'stop_code', 'geometry', 'stop_name', 'route_short_name', 'route_desc',
                                                'route_type', 'route_color', 'route_text_color'}

    moving_stops = sp_io.read_parquet("fake_moving_stops.parq")
    assert moving_stops.shape == (436, 11)
    assert set(moving_stops.columns.tolist()) == {'start_date', 'end_date', "geometry", 'stop_code', 'x', 'y',
                                                  'stop_name', 'route_type', 'route_long_name', 'route_short_name',
                                                  'direction_id'}


def test_data_processing_full_data_tresh_1():
    # Warning let this test at the end of the file

    base_lines = sp_io.read_parquet("fake_base_lines_data.parq")
    assert base_lines.shape == (7, 8)
    assert set(base_lines.columns.tolist()) == {'shape_id', 'geometry', 'route_desc', 'route_type', 'route_short_name',
                                                'direction_id', 'route_color', 'route_text_color'}

    base_stops = sp_io.read_parquet("fake_base_stops_data.parq")
    assert base_stops.shape == (8, 8)
    assert set(base_stops.columns.tolist()) == {'stop_code', 'geometry', 'stop_name', 'route_short_name', 'route_desc',
                                                'route_type', 'route_color', 'route_text_color'}

    moving_stops = sp_io.read_parquet("fake_moving_stops.parq")
    assert moving_stops.shape == (436, 11)
    assert set(moving_stops.columns.tolist()) == {'start_date', 'end_date', "geometry", 'stop_code', 'x', 'y',
                                                  'stop_name', 'route_type', 'route_long_name', 'route_short_name',
                                                  'direction_id'}
