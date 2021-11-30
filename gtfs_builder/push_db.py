from typing import Dict
from typing import List

import os
import re

from geolib import GeoLib
import numpy as np

from psycopg2.extras import DateTimeRange
from datetime import datetime

from gtfs_builder.db.base import Base
from gtfs_builder.db.stops_times_toulouse import StopsTimesToulouse
from gtfs_builder.db.stops_times_ter import StopsTimesTer

from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker

from sqlalchemy.event import listen

import spatialpandas.io as sp_io

from sqlalchemy.sql import select, func


def load_spatialite(dbapi_conn, connection_record):
    dbapi_conn.enable_load_extension(True)
    dbapi_conn.load_extension("mod_spatialite")
    dbapi_conn.enable_load_extension(False)

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
        "geom",
        "route_type",
        "stop_name",
        "pos",
        # "route_long_name",
        "route_short_name",
        "direction_id",
    ]

    __MAIN_DB_SCHEMA = "gtfs_data"
    __PG_EXTENSIONS = []

    def __init__(self, area_names: List[str]):
        super().__init__()

        self._credentials = os.environ.get("ADMIN_DB_URL")

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

        if os.environ["DROP_TABLES"] == "yes":
            # TODO remove sqlite db
            if os.path.isfile(self._credentials):
                os.remove(self._credentials)

        os.environ['PATH'] = os.environ['SPATALITE_PATH'] + ';' + os.environ['PATH']

        self._engine = create_engine(f"sqlite:///{self._credentials}", echo=True)
        listen(self._engine, 'connect', load_spatialite)
        self._session = sessionmaker(bind=self._engine)()

        if os.environ["DROP_TABLES"] == "yes":

            metadata = MetaData(self._engine)
            # self._session.execute("SELECT InitSpatialMetaData();")
            conn = self._engine.connect()
            conn.execute(select([func.InitSpatialMetaData()]))
            self._session.commit()
            Base.metadata.create_all(self._engine)



        tables = [table.fullname for table in Base.metadata.sorted_tables]
        if len(tables) > 0:
            tables_str = ', '.join(tables)
            self.logger.info(f'({len(tables)}) tables  found: {tables_str}')
        else:
            raise ValueError("No table found on DB!")

    def chunk_df(self, df_input, chunksize=100000):
        if chunksize is not None:
            for i in range(0, df_input.shape[0], chunksize):
                yield df_input[i: i + chunksize]
        else:
            yield list

    def proc_data(self):
        for area_name, data in self._data.items():
            data = data.sort_values("start_date")

            data_geom_col_renamed = data.rename(columns={"geometry_wkt": "geom"})

            if area_name == "ter":
                data_geom_col_renamed.loc[:, "direction_id"] = "null"
            data_geom_col_renamed = data_geom_col_renamed[self.__STOPS_TIMES_COLUMNS]

            # data_geom_col_renamed["start_date"] = [datetime.fromtimestamp(row.timestamp()) for row in data_geom_col_renamed["start_date"]]
            # data_geom_col_renamed["end_date"] = [datetime.fromtimestamp(row.timestamp()) for row in data_geom_col_renamed["end_date"]]

            data_geom_col_renamed["uuid"] = np.arange(len(data_geom_col_renamed))

            if area_name == "toulouse":
                table = StopsTimesToulouse
            elif area_name == "ter":
                table = StopsTimesTer
            else:
                raise ValueError("not table found")

            df_chunks = self.chunk_df(data_geom_col_renamed, 500000)
            count = 0
            for chunk in df_chunks:
                chunk["geom"] = [f"SRID=4326;{row}" for row in chunk["geom"]]
                row_to_add = [
                    table(**row
                        # uuid=row["uuid"],
                        # stop_code=row["stop_code"],
                        # start_date=row["start_date"],
                        # end_date=row["end_date"],
                        # geom=f"SRID=4326;{row['geom']}",
                        # route_type=row["route_type"],
                        # stop_name=row["stop_name"],
                        # pos=row["pos"],
                        # route_short_name=row["route_short_name"],
                        # direction_id=row["direction_id"],
                    )
                    for row in chunk.to_dict("records")
                ]
                self._session.add_all(row_to_add)
                self._session.commit()
                count += len(row_to_add)

                self.logger.info(f"{count} / {data.shape[0]} written")

    @staticmethod
    def _format_validity_range(start_date=None, end_date=None):
        if start_date is None:
            start_date = datetime.min
        if end_date is None:
            end_date = datetime.max
        return DateTimeRange(start_date, end_date)
