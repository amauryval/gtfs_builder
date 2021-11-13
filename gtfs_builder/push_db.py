from typing import Dict
from typing import List

import os
import re

from geolib import GeoLib

from psycopg2.extras import DateTimeRange
from datetime import datetime

from gtfs_builder.db.base import Base
from gtfs_builder.db.stops_times_toulouse import StopsTimesToulouse
from gtfs_builder.db.stops_times_ter import StopsTimesTer

from shapely.geometry import Point

import spatialpandas.io as sp_io


def str_to_dict_from_regex(string_value, regex):
    pattern = re.compile(regex)
    extraction = pattern.match(string_value)
    return extraction.groupdict()


class PushDb(GeoLib):

    __STOPS_TIMES_COLUMNS = [
        "stop_code",
        # "study_area_name",
        "start_date",
        "end_date",
        "geometry",
        "route_type",
        "stop_name",
        "pos",
        # "route_long_name",
        "route_short_name",
        "direction_id",
    ]

    __MAIN_DB_SCHEMA = "gtfs_data"
    __PG_EXTENSIONS = ["btree_gist", "postgis"]

    def __init__(self, area_names: List[str]):
        super().__init__()

        self._credentials = {
            **str_to_dict_from_regex(
                os.environ.get("ADMIN_DB_URL"),
                ".+:\/\/(?P<username>.+):(?P<password>.+)@(?P<host>[\W\w-]+):(?P<port>\d+)\/(?P<database>.+)"
            ),
        }

        self._area_names = area_names

    def run(self):

        self._prepare_data()
        self._prepare_db()
        self.proc_data()

    def _prepare_data(self):

        self._data = {
            area_name: sp_io.read_parquet(f"{area_name}_moving_stops.parq")
            for area_name in self._area_names
        }

    def _prepare_db(self):
        self.logger.info('Prepare database')

        db_sessions = self.init_db(
            **self._credentials,
            extensions=self.__PG_EXTENSIONS,
            overwrite=False
        )
        self._engine = db_sessions["engine"]
        schemas = Base.metadata._schemas
        for schema in schemas:
            self.init_schema(self._engine, schema)
        if os.environ["DROP_TABLES"] == "yes":
            Base.metadata.drop_all(self._engine)
            Base.metadata.create_all(self._engine)

        tables = [table.fullname for table in Base.metadata.sorted_tables]
        if len(tables) > 0:
            tables_str = ', '.join(tables)
            self.logger.info(f'({len(tables)}) tables  found: {tables_str}')
        else:
            raise ValueError("Not tables found on DB!")

    def _get_db_session(self):
        session, engine = self.sqlalchemy_connection(**self._credentials)
        return session, engine

    def proc_data(self):
        for area_name, data in self._data.items():
            data = data.sort_values("start_date")

            # data["geometry"] = [Point(row.flat_values) for row in data["geometry"]]
            # data = data.to_geopandas()
            data_cleaned = data.drop(columns=["geometry"])
            data_geom_col_renamed = data_cleaned.rename(columns={"geometry_wkb": "geometry"})

            data_filtered = data_geom_col_renamed[self.__STOPS_TIMES_COLUMNS]
            if area_name == "ter":
                data_filtered.loc[:, "direction_id"] = "null"

            # input_data = self.gdf_design_checker(self._engine, self.__MAIN_DB_SCHEMA, StopsTimes.__table__.name, data, epsg=4326)
            dict_data = self.df_to_dicts_list(data_filtered, 4326)

            if area_name == "toulouse":
                table = StopsTimesToulouse
            elif area_name == "ter":
                table = StopsTimesTer

            self.dict_list_to_db(self._engine, dict_data, self.__MAIN_DB_SCHEMA, table.__table__.name)

    @staticmethod
    def _format_validity_range(start_date=None, end_date=None):
        if start_date is None:
            start_date = datetime.min
        if end_date is None:
            end_date = datetime.max
        return DateTimeRange(start_date, end_date)
