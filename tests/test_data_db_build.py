from gtfs_builder.main import GtfsFormater

import datetime

from gtfs_builder.db.moving_points import MovingPoints

from shapely.wkt import loads


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
    assert MovingPoints.infos().rows_count == 407

    date = datetime.datetime.strptime('01/01/2007 08:02:40', '%d/%m/%Y %H:%M:%S')
    query = MovingPoints.filter_by_date_area(date, "fake", (-180, -89, 180, 89))
    assert query.count() == 1

    query = MovingPoints.get_bounds_by_area("fake")
    assert query.count() == 1
    assert len(MovingPoints.get_bounds_by_area("fake").first()) == 3
    assert loads(MovingPoints.get_bounds_by_area("fake").first()[0]).bounds == (-122.482, 36.881, -116.752, 37.658)
    assert MovingPoints.get_bounds_by_area("fake").first()[1] == datetime.datetime(2007, 1, 1, 6, 0)
    assert MovingPoints.get_bounds_by_area("fake").first()[2] == datetime.datetime(2007, 1, 1, 12, 15)


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
    assert MovingPoints.infos().rows_count == 407

    date = datetime.datetime.strptime('04/06/2007 08:00:20', '%d/%m/%Y %H:%M:%S')
    query = MovingPoints.filter_by_date_area(date, "fake", (-180, -89, 180, 89))
    assert query.count() == 1

    query = MovingPoints.get_bounds_by_area("fake")
    assert query.count() == 1
    assert len(MovingPoints.get_bounds_by_area("fake").first()) == 3
    assert loads(MovingPoints.get_bounds_by_area("fake").first()[0]).bounds == (-122.482, 36.881, -116.752, 37.658)
    assert MovingPoints.get_bounds_by_area("fake").first()[1] == datetime.datetime(2007, 6, 4, 6, 0)
    assert MovingPoints.get_bounds_by_area("fake").first()[2] == datetime.datetime(2007, 6, 4, 12, 15)


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
    assert MovingPoints.infos().rows_count == 735

    date = datetime.datetime.strptime('01/01/2007 08:02:40', '%d/%m/%Y %H:%M:%S')
    query = MovingPoints.filter_by_date_area(date, "fake", (-180, -89, 180, 89))
    assert query.count() == 1

    query = MovingPoints.get_bounds_by_area("fake")
    assert query.count() == 1
    assert len(MovingPoints.get_bounds_by_area("fake").first()) == 3
    assert loads(MovingPoints.get_bounds_by_area("fake").first()[0]).bounds == (-117.132, 36.427, -116.752, 36.916)
    assert MovingPoints.get_bounds_by_area("fake").first()[1] == datetime.datetime(2007, 1, 1, 6, 0)
    assert MovingPoints.get_bounds_by_area("fake").first()[2] == datetime.datetime(2007, 1, 1, 12, 15)


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
    assert MovingPoints.infos().rows_count == 256

    date = datetime.datetime.strptime('01/01/2007 08:01:40', '%d/%m/%Y %H:%M:%S')
    query = MovingPoints.filter_by_date_area(date, "fake", (-180, -89, 180, 89))
    assert query.count() == 2

    query = MovingPoints.get_bounds_by_area("fake")
    assert query.count() == 1
    assert len(MovingPoints.get_bounds_by_area("fake").first()) == 3
    assert loads(MovingPoints.get_bounds_by_area("fake").first()[0]).bounds == (-122.482, 36.881, -116.752, 37.657)
    assert MovingPoints.get_bounds_by_area("fake").first()[1] == datetime.datetime(2007, 1, 1, 6, 0)
    assert MovingPoints.get_bounds_by_area("fake").first()[2] == datetime.datetime(2007, 1, 1, 16, 0)

