import os
from gtfs_builder.main import GtfsFormater

import datetime

from gtfs_builder.db.moving_points import MovingPoints


def test_data_processing_full_data_thresh_overwrite_table(credentials, session_db):
    GtfsFormater(
        study_area_name=credentials["study_area_name"],
        data_path=credentials["input_data_dir"],
        transport_modes=credentials["transport_modes"],
        date_mode=credentials["date_mode"],
        date=credentials["date"],
        build_shape_data=False,
        interpolation_threshold=500,
        output_format="db",
        db_mode="overwrite"  # append mode should be ok
    )
    MovingPoints.set_session(session_db)
    assert MovingPoints.schemas() == {"gtfs_data"}
    assert MovingPoints.infos().rows_count == 436

    date = datetime.datetime.strptime('01/01/2007 08:02:40', '%d/%m/%Y %H:%M:%S')
    query = MovingPoints.filter_by_date_area(date, "fake")
    assert query.count() == 1
    assert MovingPoints.get_bounds_by_area("fake") == (-122.482, 36.425, -116.752, 37.659)


def test_data_processing_full_data_calendar_dates(credentials, session_db):
    GtfsFormater(
        study_area_name=credentials["study_area_name"],
        data_path=credentials["input_data_dir"],
        transport_modes=credentials["transport_modes"],
        date_mode="calendar_dates",
        date=credentials["date"],
        build_shape_data=False,
        interpolation_threshold=500,
        output_format="db",
        db_mode="overwrite"
    )
    MovingPoints.set_session(session_db)
    assert MovingPoints.schemas() == {"gtfs_data"}
    assert MovingPoints.infos().rows_count == 436

    date = datetime.datetime.strptime('04/06/2007 08:00:20', '%d/%m/%Y %H:%M:%S')
    query = MovingPoints.filter_by_date_area(date, "fake")
    assert query.count() == 1
    assert MovingPoints.get_bounds_by_area("fake") == (-122.482, 36.425, -116.752, 37.659)



def test_data_processing_with_shape_id_computed(credentials, session_db):
    GtfsFormater(
        study_area_name=credentials["study_area_name"],
        data_path=credentials["input_data_dir"],
        transport_modes=credentials["transport_modes"],
        date_mode=credentials["date_mode"],
        date=credentials["date"],
        build_shape_data=True,
        interpolation_threshold=200,
        output_format="db",
        db_mode="overwrite"
    )
    MovingPoints.set_session(session_db)
    assert MovingPoints.schemas() == {"gtfs_data"}
    assert MovingPoints.infos().rows_count == 764

    date = datetime.datetime.strptime('01/01/2007 08:02:40', '%d/%m/%Y %H:%M:%S')
    query = MovingPoints.filter_by_date_area(date, "fake")
    assert query.count() == 1
    assert MovingPoints.get_bounds_by_area("fake") == (-117.133, 36.425, -116.752, 36.916)



def test_data_processing_full_data_tresh_1(credentials, session_db):
    # Warning let this test at the end of the file
    GtfsFormater(
        study_area_name=credentials["study_area_name"],
        data_path=credentials["input_data_dir"],
        transport_modes=credentials["transport_modes"],
        date_mode=credentials["date_mode"],
        date="20070603",
        build_shape_data=False,
        interpolation_threshold=1000,
        output_format="db",
        db_mode="overwrite"
    )
    MovingPoints.set_session(session_db)
    assert MovingPoints.schemas() == {"gtfs_data"}
    assert MovingPoints.infos().rows_count == 300

    date = datetime.datetime.strptime('01/01/2007 08:01:40', '%d/%m/%Y %H:%M:%S')
    query = MovingPoints.filter_by_date_area(date, "fake")
    assert query.count() == 2
    assert MovingPoints.get_bounds_by_area("fake") == (-122.482, 36.425, -116.401, 37.659)

