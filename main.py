
from geotools_expert.geotools import GeoTools
from itertools import chain
import numexpr

from gtfs_builder.gtfs_core.optim_helper import DfOptimizer

from gtfs_builder.gtfs_cp_inputs.shapes_in import Shapes
from gtfs_builder.gtfs_cp_inputs.stops_in import Stops
from gtfs_builder.gtfs_cp_inputs.stop_times_in import StopsTimes
from gtfs_builder.gtfs_cp_inputs.trips_in import Trips
from gtfs_builder.gtfs_cp_inputs.calendar_in import Calendar
from gtfs_builder.gtfs_cp_inputs.routes_in import Routes

from shapely.ops import split

from shapely.geometry import Point
from shapely.geometry import LineString

import numpy as np

import pandas as pd

import datetime

import geopandas

from gtfs_builder.gtfs_db.base import Base
from gtfs_builder.gtfs_db.stops import StopsMapped


class GtfsFormater(GeoTools):

    __SUB_STOPS_RESOLUTION = 50
    __DAYS_MAPPING = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ]

    __DEFAULT_END_STOP_NAME = "Start"
    __DEFAULT_VALUE_START_STOP = 9999

    _OUTPUT_STOPS_POINTS = []

    def __init__(self, credentials, overwriting_db_mode):
        super().__init__()

        self._credentials = credentials
        self._overwriting_db_mode = overwriting_db_mode

        self.run()

    def run(self):
        self._prepare_db()
        self._prepare_inputs()
        self._build_stops_data()
        self._build_path()

    def _prepare_db(self):
        # TODO add crash exception
        self.lgc_info('Create Db')

        overwrite_db = False
        if self._overwriting_db_mode == "full":
            overwrite_db = True

        pg_con = self.db_sql_alchemy_pg_init(
            **self._credentials,
            overwrite=overwrite_db,
            verbose=True
        )
        self._engine = pg_con['engine']
        self._session = pg_con['session']

        if self._overwriting_db_mode == "tables":
            Base.metadata.drop_all(self._engine)

        schemas = Base.schemas()
        for schema in schemas:
            self.db_schema_init(self._engine, schema)

        Base.metadata.create_all(self._engine)
        for table in Base.tables():
            self.lgc_info(f"Table  created: {table}")


        StopsMapped.set_session(self._session)
        self._schema_name = StopsMapped.infos().schema
        self._table_name = StopsMapped.infos().name

    def _prepare_inputs(self):

        self._shapes_data = Shapes(self).data
        self._stops_data = Stops(self).data
        self._stop_times_data = StopsTimes(self).data
        self._trips_data = Trips(self).data
        self._calendar_data = Calendar(self).data
        self._routes_data = Routes(self).data

    def _build_stops_data(self):
        self._stop_times_data.set_index("stop_id", inplace=True)
        self._stops_data.set_index("stop_id", inplace=True)
        stops_data = self._stops_data.join(self._stop_times_data).copy()
        stops_data.reset_index(inplace=True)

        stops_data.set_index("trip_id", inplace=True)
        self._trips_data.set_index("trip_id", inplace=True)
        stops_trips_data = stops_data.join(self._trips_data).copy()
        stops_trips_data.reset_index(inplace=True)

        stops_trips_data.set_index("route_id", inplace=True)
        self._routes_data.set_index("route_id", inplace=True)
        stops_trips_routes_data_complete = stops_trips_data.join(self._routes_data).copy()
        stops_trips_routes_data_complete.reset_index(inplace=True)

        data_otptimized = DfOptimizer(stops_trips_routes_data_complete)
        self._stops_data_build = data_otptimized.data

    def _build_path(self):
        self.lgc_info("Go to path building")

        end_date = max(self._calendar_data["end_date"].to_list())
        start_date = min(self._calendar_data["start_date"].to_list())

        service_ids_to_proceed_by_day = {}
        while start_date <= end_date:
            start_date_day = self.__DAYS_MAPPING[start_date.weekday()]
            service_id_working = self._calendar_data.loc[
                (self._calendar_data[start_date_day] == "1") & ((self._calendar_data["start_date"] >= start_date) | (self._calendar_data["end_date"] <= start_date))
            ]["service_id"].to_list()

            service_ids_to_proceed_by_day[start_date] = (start_date_day, service_id_working)

            start_date += datetime.timedelta(days=1)

        self._object_line = pd.DataFrame(columns=['id', 'line_stop_geom', 'interpolated_points'])
        self._object_line["id"] = self._object_line["id"].astype('category')

        # EACH DAY
        for date, (start_date_day, service_ids_working) in service_ids_to_proceed_by_day.items():
            stops_data_build_filter_by_a_day = self._stops_data_build.loc[self._stops_data_build["service_id"].isin(service_ids_working)].copy()
            shapes_data_concerned_stops_data = self._shapes_data.loc[self._shapes_data["shape_id"].isin(stops_data_build_filter_by_a_day["shape_id"].to_list())].copy()

            self.lgc_info(f">>> WORKING DAY - {start_date_day} {date} - {shapes_data_concerned_stops_data.shape[0]} line(s) found")

            processes = []
            for count, line in enumerate(shapes_data_concerned_stops_data.itertuples()):
                shape_id_line = line.shape_id
                if shape_id_line == "4503603929098653":
                    a = 0
                line_stops = stops_data_build_filter_by_a_day.loc[stops_data_build_filter_by_a_day["shape_id"] == shape_id_line].copy()

                line_caracteristics = np.unique(line_stops[["route_type", "route_short_name"]].values)
                if len(line_caracteristics) > 2:
                    raise ValueError(
                        f"line name proceed should be unique (count: {len(line_caracteristics)} ; {','.join(line_caracteristics)}")
                caract1, caract2 = line_caracteristics
                self.lgc_info(f"> ({count}) working line {shape_id_line} ({caract1}: {caract2})")
                line_stops.sort_values(by=["trip_id", "stop_sequence"], inplace=True)

                self._line_stops_computed = []
                self._run_each_stop_from_each_line(date, line_stops, line)
                # processes.append([self._run_each_stop_from_each_line, date, stops_data_build_filter_by_a_day, line])

                # self.run_threads(processes, 8)

                # gdf = pd.concat(self._line_stops_computed, ignore_index=True).sort_values(by=["date_time"], ascending=True)
                gdf = geopandas.GeoDataFrame(self._line_stops_computed, geometry=self._line_stops_computed["geom"])
                gdf.drop(columns=["geom"], inplace=True)

                data = self.gdf_design_checker(self._engine, self._schema_name, self._table_name, gdf, 2154)
                dict_data = self.df_to_dicts_list(data, 2154)
                self.db_write_dict_list_into_db(self._engine, dict_data, self._schema_name, self._table_name)

                # gdf.to_file(f"data_one4.gpkg", driver="GPKG", layer=f"{start_date_day}")
            assert False

    def _run_each_stop_from_each_line(self, date, line_stops, line):

        objects_line = {}
        line_stops_computed = []

        trips_to_proceed = set(line_stops["trip_id"].to_list())

        for trip_id in trips_to_proceed:
            print(trip_id)
            if trip_id == "4503603929098662":
                a = 0


            trip_stops = line_stops.loc[line_stops["trip_id"] == trip_id]

            line_geom_remaining = line.geometry
            start_stops = self.__DEFAULT_VALUE_START_STOP
            aa = []
            for stop_position, stop in enumerate(trip_stops.itertuples()):

                default_common_attributes = {
                    "stop_type": stop.route_type,
                    "line_name": stop.route_long_name,
                    "line_name_short": stop.route_short_name,
                    "direction_id": stop.direction_id,
                    "line_id": line.shape_id,
                    "trip_id": trip_id,
                }

                start_stop_code = self.__DEFAULT_VALUE_START_STOP if start_stops == self.__DEFAULT_VALUE_START_STOP else start_stops["stop_code"]

                if start_stop_code == "20031":
                    a = 0
                    pass
                hours, minutes, seconds = map(int, stop.arrival_time.split(":"))
                arrival_time = date + datetime.timedelta(seconds=seconds, minutes=minutes, hours=hours)
                start_date = arrival_time - datetime.timedelta(minutes=1) if start_stops == self.__DEFAULT_VALUE_START_STOP else start_stops["date_time"]
                end_date = arrival_time

                # sometimes a stop appears at the same time with the last
                if start_date < end_date:

                    object_id = f"{start_stop_code}_{stop.stop_code}"
                    # print(object_id)
                    # is_data_found = objects_line.get(f"{start_stop_code}_{stop.stop_code}", None)
                    is_data_found = self._object_line.loc[self._object_line["id"] == object_id]
                    if is_data_found.shape[0] == 1:
                        line_stop_geom = is_data_found["line_stop_geom"].iloc[0]
                        interpolated_points = is_data_found["interpolated_points"].iloc[0]

                    else:
                        line_stop_geom, line_geom_remaining = self._get_dedicated_line_from_stop(stop, line_geom_remaining)

                        # data interpolated
                        interpolation_value = int(line_stop_geom.length / self.__SUB_STOPS_RESOLUTION) #create func
                        interpolated_points = tuple(
                            line_stop_geom.interpolate(value, normalized=True)
                            for value in np.linspace(0, 1, interpolation_value)
                        )[1:-1]

                        new_entry = pd.DataFrame([{
                            "id": object_id,
                            "line_stop_geom": line_stop_geom,
                            "interpolated_points": interpolated_points
                        }])
                        new_entry["id"] = new_entry["id"].astype('category')
                        self._object_line = pd.concat([self._object_line, new_entry])

                        # objects_line[f"{start_stop_code}_{stop.stop_code}"] = (line_stop_geom, interpolated_points)

                    start_point = Point(line_stop_geom.coords[0])
                    end_point = Point(line_stop_geom.coords[-1])

                    start_stops = {
                        "date_time": start_date,
                        "stop_code": start_stop_code,
                        "geom": start_point if start_stops == self.__DEFAULT_VALUE_START_STOP else start_stops["geom"],
                        "stop_name": self.__DEFAULT_END_STOP_NAME if start_stops == self.__DEFAULT_VALUE_START_STOP else start_stops["stop_name"],
                        "pos": stop_position
                    }
                    start_stops.update(default_common_attributes)

                    last_stops = {
                        "date_time": end_date,
                        "stop_code": stop.stop_code,
                        "geom": end_point,
                        "stop_name": stop.stop_name,
                        "pos": stop_position + 1
                    }
                    line_stops_computed.append(start_stops)
                    aa.append(start_stops)
                    start_stops = last_stops

                    interpolation_value = int(line_stop_geom.length / self.__SUB_STOPS_RESOLUTION)
                    interpolated_datetime = pd.date_range(start_date, end_date, periods=interpolation_value).to_list()[1:-1]

                    interpolated_data = zip(interpolated_datetime, interpolated_points)

                    for sub_stop_position, (date_time, point) in enumerate(interpolated_data):
                        new_point = {
                            "date_time": date_time,
                            "stop_code": None,
                            "geom": point,
                            "stop_name": None,
                            "pos": f"{stop_position}.{sub_stop_position}"
                        }
                        new_point.update(default_common_attributes)
                        line_stops_computed.append(new_point)
                        aa.append(new_point)

        self._line_stops_computed = pd.DataFrame(line_stops_computed)
        # gdf = geopandas.GeoDataFrame(trip_done, geometry=trip_done["geom"])
        # gdf.drop(columns=["geom"], inplace=True)
        #
        # data = self.gdf_design_checker(self._engine, self._schema_name, self._table_name, gdf, 2154)
        # dict_data = self.df_to_dicts_list(data, 2154)
        # self.db_write_dict_list_into_db(self._engine, dict_data, self._schema_name, self._table_name)
        # self._OUTPUT_STOPS_POINTS.append(trip_done)


    def _get_dedicated_line_from_stop(self, stop, line_shape_geom_remained):

        projected_point = line_shape_geom_remained.interpolate(line_shape_geom_remained.project(stop.geometry))

        all_points_coords = chain(line_shape_geom_remained.coords, projected_point.coords)
        all_points = map(Point, all_points_coords)
        new_line = LineString(sorted(all_points, key=line_shape_geom_remained.project))
        line_splitted_result = split(new_line, projected_point)
        stop_segment = line_splitted_result[0]
        line_geom_remaining = line_splitted_result[-1]

        if line_geom_remaining.equals(stop_segment):
            stop_segment = LineString([projected_point, projected_point])

        return stop_segment, line_geom_remaining

    def _get_weekday_from_date(self, date_value, day):

        while date_value.weekday() != self.__DAYS_MAPPING[day]:
            date_value += datetime.timedelta(days=1)

        return date_value

if __name__ == '__main__':
    input_db = {
        "credentials": {
            "host": "localhost",
            "port": 5432,
            "database": "gtfs_db",
            "username": "postgres",
            "password": "postgres",
            "extensions": ["btree_gist", "postgis"]
        },
        "overwriting_data_mode": "full",
    }
    GtfsFormater(input_db["credentials"], input_db["overwriting_data_mode"])
