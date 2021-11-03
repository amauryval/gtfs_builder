from typing import List
from typing import Dict
from typing import Optional

import os
import datetime
import itertools
import copy

import shapely
from geolib import GeoLib
from itertools import chain
import numpy as np
import geopandas as gpd

from gtfs_builder.gtfs_core.optim_helper import DfOptimizer

from gtfs_builder.gtfs_cp_inputs.shapes_in import Shapes
from gtfs_builder.gtfs_cp_inputs.stops_in import Stops
from gtfs_builder.gtfs_cp_inputs.stop_times_in import StopsTimes
from gtfs_builder.gtfs_cp_inputs.trips_in import Trips
from gtfs_builder.gtfs_cp_inputs.calendar_in import Calendar
from gtfs_builder.gtfs_cp_inputs.routes_in import Routes
from gtfs_builder.gtfs_core.core import InputDataNotFound
from shapely.ops import split

from shapely.geometry import Point
from shapely.geometry import LineString

import pandas as pd

from collections import defaultdict

from psycopg2.extras import DateTimeRange

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed


from spatialpandas import GeoDataFrame



def run_thread(processes, workers_number=4):
    # TODO add to geolib

    with ThreadPoolExecutor(max_workers=workers_number) as executor:

        executions = []
        for process in processes:
            if isinstance(process, list):
                executions.append(executor.submit(*process))
            else:
                executions.append(executor.submit(process))
        # to return exceptions
        return [
            exe.result()
            for exe in as_completed(executions)
        ]

def run_process(processes, workers_number=4):
    # TODO add to geolib

    with ProcessPoolExecutor(max_workers=workers_number) as executor:

        executions = []
        for process in processes:
            if isinstance(process, list):
                executions.append(executor.submit(*process))
            else:
                executions.append(executor.submit(process))
        # to return exceptions
        return [
            exe.result()
            for exe in as_completed(executions)
        ]


def group_by_similarity(data):
    import uuid
    data["values_frozen"] = data["stop_id"].apply(frozenset)
    data.loc[:, "start_end_stops"] = None

    my_rows = zip(data["trip_id"], data["values_frozen"], data["start_end_stops"])
    output = defaultdict(lambda: defaultdict(str))

    for idx, (trip_id, values_frozen, start_end_stops) in enumerate(my_rows):
        common_id = str(uuid.uuid4())

        if trip_id not in output:
            data_not_proceed = data.loc[data.start_end_stops is not None]
            data_to_procceed = data_not_proceed.loc[
                data_not_proceed["values_frozen"].apply(lambda x: len(x.intersection(values_frozen)) >= 2)
            ]["trip_id"]
            if data_to_procceed.shape[0] > 0:
                for trip_id_value in data_to_procceed.tolist():
                    output[trip_id_value] = common_id

            else:
                output[trip_id] = common_id
        else:
            assert True

    return output


class GtfsFormater(GeoLib):
    pd.options.mode.chained_assignment = None

    __COORDS_PRECISION = 3

    __RAW_DATA_DIR = "../input_data"

    __SHAPES_FILE_CREATED_NAME = "shapes.txt"
    __TRIPS_FILE_UPDATED_NAME = "trips.txt"

    __SUB_STOPS_RESOLUTION = 25
    __DAYS_MAPPING = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ]

    __MOVING_DATA_COLUMNS = [
        "start_date",
        "end_date",
        "stop_code",
        "x",
        "y",
        "geometry",
        "stop_name",
        "pos",
        "route_type",
        "route_long_name",
        "route_short_name",
        "direction_id",
        "shape_id",
        "trip_id"
    ]

    __MOVING_STOPS_OUTPUT_PARQUET_FILE = "moving_stops.parq"
    __BASE_STOPS_OUTPUT_PARQUET_FILE = "base_stops_data.parq"
    __BASE_LINES_OUTPUT_PARQUET_FILE = "base_lines_data.parq"

    def __init__(self,
        study_area_name,
        data_path,
        transport_modes: Optional[List[str]] = None,
        days: Optional[List[str]] = None,
        build_shape_data: bool = False
    ):
        super().__init__()

        self._study_area_name = study_area_name
        self.path_data = data_path
        self._transport_modes = transport_modes
        self._days = days
        self.build_shape_data = build_shape_data

        self.run()

    def run(self):

        self._prepare_inputs()
        self._build_stops_data()
        self._build_path()

    def _prepare_inputs(self):
        self._stop_times_data = StopsTimes(self).data

        try:
            self._shapes_data = Shapes(self).data
            self._trips_data = Trips(self).data
        except InputDataNotFound:
            self._compute_shapes_txt()
            self._trips_data = Trips(self, self.__TRIPS_FILE_UPDATED_NAME).data
            self._shapes_data = Shapes(self, self.__SHAPES_FILE_CREATED_NAME).data
        finally:
            self._stops_data = Stops(self).data
            self._calendar_data = Calendar(self).data
            self._routes_data = Routes(self, transport_modes=self._transport_modes).data

    def _compute_shapes_txt(self):
        self.logger.info("Shapes computing...")
        stop_times_data = self._stop_times_data.sort_values(by=["trip_id", "stop_sequence"])
        stop_ids_from_trip_id = stop_times_data.groupby("trip_id").agg(stop_id=('stop_id', list), stop_sequence=('stop_sequence', list)).reset_index()
        stop_ids_from_trip_id["stop_count"] = stop_ids_from_trip_id["stop_id"].apply(lambda x: len(x))
        stop_ids_from_trip_id.sort_values(by=["stop_count"], ascending=[True], inplace=True)

        # find common path
        data = group_by_similarity(stop_ids_from_trip_id)
        # to df
        data_to_df = pd.DataFrame.from_dict(data, orient='index', columns=['shape_id'])
        # merge
        stop_ids_from_trip_id.set_index("trip_id", inplace=True)
        merged = stop_ids_from_trip_id.merge(data_to_df, left_index=True, right_index=True)
        merged["stop_count"] = merged["stop_id"].apply(lambda x: len(x))
        merged.sort_values(by=["shape_id", "stop_count"], ascending=[False, True], inplace=True)
        merged = merged[["shape_id", "stop_id", "stop_count", "stop_sequence"]]
        merged.reset_index(inplace=True)
        merged.rename(columns={"index": "trip_id"}, inplace=True)

        # find best geom path
        grouped_by_shape_id = merged.groupby("shape_id").agg(
            {
                "trip_id": "first",
                "stop_id": "first",
                "stop_count": "max",
                "stop_sequence": "first"
            }
        ).reset_index()
        grouped_by_shape_id = grouped_by_shape_id[["shape_id", "stop_id", "stop_sequence"]]

        output = merged[["trip_id", "shape_id"]]
        output1 = output.merge(grouped_by_shape_id, left_on='shape_id', right_on='shape_id')
        output1.rename(columns={"shape_id_x": "shape_id"}, inplace=True)

        test = output1.explode("stop_id")
        test['shape_pt_sequence'] = test.groupby(["trip_id"])["stop_id"].cumcount()
        stops = Stops(self, use_original_epsg=True).data[["stop_id", "geometry"]]
        test2 = test.merge(stops, left_on='stop_id', right_on='stop_id')
        test2 = test2.sort_values(by=["trip_id", "shape_pt_sequence"])
        test2["shape_pt_lat"] = test2["geometry"].apply(lambda x: x.y)
        test2["shape_pt_lon"] = test2["geometry"].apply(lambda x: x.x)

        shapes_updated = test2
        shapes_updated.to_csv(os.path.join(self.__RAW_DATA_DIR, self.__SHAPES_FILE_CREATED_NAME), index=False)

        # upd trips
        shapes_updated_grouped = shapes_updated[["trip_id", "shape_id"]].groupby("trip_id").agg({'shape_id':'first'}).reset_index()
        trips = Trips(self).data

        if "shape_id" not in trips.columns:
            trips.drop(columns=["shape_id"], inplace=True)
        trips_updated = trips.merge(shapes_updated_grouped, on="trip_id")

        shapes_updated.drop(columns=["trip_id", "geometry", "stop_id"], inplace=True)

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
        stops_trips_routes_data_complete = stops_trips_data.merge(self._routes_data, on="route_id", how="right").copy()
        stops_trips_routes_data_complete.reset_index(inplace=True)

        data_otptimized = DfOptimizer(stops_trips_routes_data_complete)
        self._stops_data_build = data_otptimized.data

    def _build_path(self):
        self.logger.info("Go to path building")

        end_date = self._calendar_data["end_date"].max()
        start_date = self._calendar_data["start_date"].min()

        service_ids_to_proceed_by_day = {}
        while start_date <= end_date:
            start_date_day = self.__DAYS_MAPPING[start_date.weekday()]
            service_id_working = self._calendar_data.loc[
                (self._calendar_data[start_date_day] == "1") & ((self._calendar_data["start_date"] >= start_date) | (self._calendar_data["end_date"] <= start_date))
            ]["service_id"].to_list()

            service_ids_to_proceed_by_day[start_date] = (start_date_day, service_id_working)

            start_date += datetime.timedelta(days=1)

        # initializing interpolated points cache
        self._temp_interpolated_points_cache = {}

        # EACH DAY
        for date, (start_date_day, service_ids_working) in service_ids_to_proceed_by_day.items():

            stops_on_day = self._stops_data_build.loc[self._stops_data_build["service_id"].isin(service_ids_working)].copy()
            lines_on_day = self._shapes_data.loc[self._shapes_data["shape_id"].isin(stops_on_day["shape_id"].to_list())].copy()

            self.logger.info(f">>> WORKING DAY - {start_date_day} {date} - {lines_on_day.shape[0]} line(s) day found")
            self._line_trip_ids_stops_computed = []

            self.compute_moving_geom(stops_on_day, lines_on_day, date)
            self.compute_fixed_geom(stops_on_day, lines_on_day)

            return

    def compute_moving_geom(self, stops_on_day, lines_on_day, date):

        stops_on_day["arrival_time"] = stops_on_day["arrival_time"].map(lambda x: self._compute_date(date, x))
        stops_on_day["departure_time"] = stops_on_day["departure_time"].map(lambda x: self._compute_date(date, x))
        stops_on_day["geometry"] = stops_on_day["geometry"].map(lambda x: self._compute_geom_precision(x, self.__COORDS_PRECISION))
        stops_on_day["x"] = stops_on_day["geometry"].x
        stops_on_day["y"] = stops_on_day["geometry"].y
        stops_on_day = stops_on_day.rename({'arrival_time': 'start_date', 'departure_time': 'end_date'}, axis=1)

        processes = [
            [self.compute_line, line, stops_on_day, date]
            for line in lines_on_day.to_dict('records')
        ]
        res = run_process(processes)
        self.logger.info(f"{len(res)}")
        data_completed = itertools.chain(*res)

        # data_completed = []
        # for line in lines_on_day.to_dict('records'):
        #     data_completed.append(self.compute_line(line, stops_on_day, date))

        data = gpd.GeoDataFrame(data_completed)
        data = data[self.__MOVING_DATA_COLUMNS]

        data_sp = GeoDataFrame(data)
        data_sp["start_date"] = data_sp["start_date"].apply(lambda x: int(x.timestamp()))
        data_sp["end_date"] = data_sp["end_date"].apply(lambda x: int(x.timestamp()))

        data_sp.to_parquet(f"{self._study_area_name}_{self.__MOVING_STOPS_OUTPUT_PARQUET_FILE}")

    def compute_fixed_geom(self, stops_data, lines_data):
        stops_data_copy = stops_data.copy(deep=True)
        stops = stops_data_copy.groupby(["stop_code"], sort=False).agg({
            "stop_code": "first",
            "geometry": "first",
            "stop_name": lambda x: list(set(list(x))),
            "route_short_name": lambda x: list(set(list(x))),
            "route_desc": lambda x: list(set(list(x))),
            "route_type": lambda x: list(set(list(x))),
            "route_color": lambda x: list(set(list(x))),
            "route_text_color": lambda x: list(set(list(x))),
        }).dropna()
        stops_data = gpd.GeoDataFrame(stops)
        data_sp = GeoDataFrame(stops_data)
        data_sp.to_parquet(f"{self._study_area_name}_{self.__BASE_STOPS_OUTPUT_PARQUET_FILE}")

        lines = stops_data_copy[
            ["shape_id", "route_desc", "route_type", "route_short_name", "direction_id", "route_color", "route_text_color"]
        ].merge(
            lines_data, on="shape_id", how="right"
        ).groupby(["shape_id"]).agg({
            "shape_id": "first",
            "geometry": "first",
            "route_desc": lambda x: list(set(list(x))),
            "route_type": lambda x: list(set(list(x))),
            "route_short_name": lambda x: list(set(list(x))),
            "direction_id": lambda x: list(set(list(x))),
            "route_color": lambda x: list(set(list(x))),
            "route_text_color": lambda x: list(set(list(x))),
        })
        lines_data = gpd.GeoDataFrame(lines)
        data_sp = GeoDataFrame(lines_data)
        data_sp.to_parquet(f"{self._study_area_name}_{self.__BASE_LINES_OUTPUT_PARQUET_FILE}")

    def compute_line(self, line, stops_on_day, date):
        input_line_id = line["shape_id"]
        line_stops = self._get_stops_line(input_line_id, stops_on_day)

        trips_to_proceed = set(line_stops["trip_id"].to_list())
        for trip_id in trips_to_proceed:
            self.compute_trip(date, line, line_stops, trip_id)

        return self._line_trip_ids_stops_computed

    def compute_trip(self, date, line, line_stops, trip_id):
        trip_stops = line_stops.loc[line_stops["trip_id"] == trip_id]
        trip_stops_computed = self._build_interpolation_stops_on_trip(date, trip_id, trip_stops, line)
        self._line_trip_ids_stops_computed.extend(trip_stops_computed)

    def _get_stops_line(self, input_line_id, stops_on_day):

        stops_line = stops_on_day.loc[stops_on_day["shape_id"] == input_line_id].copy()

        line_caracteristics = np.unique(stops_line[["route_type", "route_short_name"]].values)
        if len(line_caracteristics) > 2:
            raise ValueError(
                f"line name proceed should be unique (count: {len(line_caracteristics)} ; {','.join(line_caracteristics)}")
        caract1, caract2 = line_caracteristics
        self.logger.info(f"> Working line {input_line_id} ({caract1}: {caract2})")
        stops_line.sort_values(by=["trip_id", "stop_sequence"], inplace=True)

        return stops_line

    @staticmethod
    def compute_wg84_line_length(input_geom):
        """
        Compute the length of a wg84 line (LineString and MultiLineString)
        :param input_geom: input geometry
        :type input_geom: shapely.geometry.LineString or shapely.geometry.MultiLineString
        :return: the line length
        :rtype: float
        """
        from pyproj import Geod

        line_length = Geod(ellps="WGS84").geometry_length(input_geom)

        return line_length

    @staticmethod
    def _compute_date(date_to_add, feature_date):
        hours, minutes, seconds = map(int, feature_date.split(":"))
        return date_to_add + datetime.timedelta(seconds=seconds, minutes=minutes, hours=hours)

    @staticmethod
    def _compute_geom_precision(feature_geom, precision):
        return shapely.wkt.loads(shapely.wkt.dumps(feature_geom, rounding_precision=precision))

    def _build_interpolation_stops_on_trip(self, date, trip_id, trip_stops, line):

        line_geom_remaining = line["geometry"]

        trip_stops_computed = []

        trip_stops.insert(loc=0, column='pos', value=np.arange(len(trip_stops)))
        trip_stops_elements = trip_stops.to_dict('records')
        stop_pairs = list(zip(trip_stops_elements, trip_stops_elements[1:]))

        for pair in stop_pairs:
            first_stop = pair[0]
            next_stop = pair[-1]

            start_date = first_stop["end_date"]
            end_date = next_stop["start_date"]

            # no need the first and the last to avoid duplicates
            interpolated_datetime = pd.date_range(start_date, end_date, periods=self.__SUB_STOPS_RESOLUTION).to_list()[1:-1]
            interpolated_datetime_pairs = list(zip(interpolated_datetime, interpolated_datetime[1:]))

            object_id = f"{first_stop['stop_code']}_{next_stop['stop_code']}"
            if object_id in self._temp_interpolated_points_cache:
                line_geom_remaining = self._temp_interpolated_points_cache[object_id]["line_geom_remaining"]
                interpolated_points = self._temp_interpolated_points_cache[object_id]["interpolated_points"]

            else:
                line_stop_geom, line_geom_remaining = self._get_dedicated_line_from_stop(next_stop, line_geom_remaining)
                interpolated_points = tuple(
                    line_stop_geom.interpolate(value, normalized=True)
                    for value in np.linspace(0, 1, self.__SUB_STOPS_RESOLUTION)
                )[1:-1]

                self._temp_interpolated_points_cache[object_id] = {
                    "interpolated_points": interpolated_points,
                    "line_geom_remaining": line_geom_remaining
                }

            interpolated_data = list(zip(interpolated_datetime_pairs, interpolated_points))

            for sub_stop_position, (date_time, point_geom) in enumerate(interpolated_data, start=1):
                new_stop = copy.deepcopy(first_stop)
                geom = self._compute_geom_precision(point_geom, self.__COORDS_PRECISION)
                new_stop.update(
                    {
                        'end_date': date_time[-1],
                        'start_date': date_time[0],
                        'geometry': geom,
                        'x': geom.x,
                        'y': geom.y,
                        'pos': new_stop['pos'] + sub_stop_position / 100,
                    }
                )
                trip_stops_elements.append(new_stop)

        trip_stops_computed.extend(trip_stops_elements)

        return trip_stops_computed

    def _get_dedicated_line_from_stop(self, stop: Dict, line_shape_geom_remained):

        projected_point = line_shape_geom_remained.interpolate(line_shape_geom_remained.project(stop["geometry"]))

        all_points_coords = chain(line_shape_geom_remained.coords, projected_point.coords)
        all_points = map(Point, all_points_coords)
        new_line = LineString(sorted(all_points, key=line_shape_geom_remained.project))

        #TODO useful ? it's working!
        # lines_coords = list(new_line.coords)
        # split_index = lines_coords.index(projected_point.coords[0])
        # stop_segment = LineString(lines_coords[:split_index + 1])
        # line_geom_remaining = LineString(lines_coords[split_index:])

        line_splitted_result = split(new_line, projected_point)
        stop_segment = line_splitted_result.geoms[0]
        line_geom_remaining = line_splitted_result.geoms[-1]

        if line_geom_remaining.equals(stop_segment):
            stop_segment = LineString([projected_point, projected_point])

        return stop_segment, line_geom_remaining

    def _get_weekday_from_date(self, date_value, day):

        while date_value.weekday() != self.__DAYS_MAPPING[day]:
            date_value += datetime.timedelta(days=1)

        return date_value

    @staticmethod
    def _format_validity_range(start_date=None, end_date=None):
        if start_date is None:
            start_date = datetime.min
        if end_date is None:
            end_date = datetime.max
        return DateTimeRange(start_date, end_date)



