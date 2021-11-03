

from gtfs_builder.main import GtfsFormater

import spatialpandas.io as sp


def test_data_processing(credentials):
    GtfsFormater(
        credentials["study_area_name"],
        credentials["path_data"],
        credentials["transport_modes"],
        credentials["days"],
    )
    base_lines = sp.read_parquet("fake_base_lines_data.parq")
    assert base_lines.shape == (11, 8)

    base_stops = sp.read_parquet("fake_base_stops_data.parq")
    assert base_stops.shape == (1, 8)

    moving_stops = sp.read_parquet("fake_moving_stops.parq")
    assert moving_stops.shape == (306, 14)
