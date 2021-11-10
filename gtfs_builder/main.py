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

from gtfs_builder.core.optim_helper import DfOptimizer

from gtfs_builder.inputs_builders.shapes_in import Shapes
from gtfs_builder.inputs_builders.stops_in import Stops
from gtfs_builder.inputs_builders.stop_times_in import StopsTimes
from gtfs_builder.inputs_builders.trips_in import Trips
from gtfs_builder.inputs_builders.calendar_in import Calendar
from gtfs_builder.inputs_builders.calendar_dates_in import CalendarDates
from gtfs_builder.inputs_builders.routes_in import Routes
from shapely.ops import split

from shapely.geometry import Point
from shapely.geometry import LineString

import pandas as pd

import hashlib

from psycopg2.extras import DateTimeRange

from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

from itertools import accumulate
import operator

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


class ShapeIdError(Exception):
    pass


class GtfsFormater(GeoLib):
    pd.options.mode.chained_assignment = None

    __COORDS_PRECISION = 3

    __RAW_DATA_DIR = "../input_data"

    __SHAPES_FILE_CREATED_NAME = "shapes_computed.txt"
    __TRIPS_FILE_UPDATED_NAME = "trips_updated.txt"

    __SUB_STOPS_RESOLUTION = 200  # 1pt for each 25 meters
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
        "route_type",
        "route_long_name",
        "route_short_name",
        "direction_id",
    ]

    __MOVING_STOPS_OUTPUT_PARQUET_FILE = "moving_stops.parq"
    __BASE_STOPS_OUTPUT_PARQUET_FILE = "base_stops_data.parq"
    __BASE_LINES_OUTPUT_PARQUET_FILE = "base_lines_data.parq"

    def __init__(self,
        study_area_name,
        data_path,
        transport_modes: Optional[List[str]] = None,
        date_mode: str = "calendar",
        date: str = None,
        build_shape_data: bool = False,
        interpolation_threshold: int = 1000,
        multiprocess: bool = False
    ):
        super().__init__()

        self._study_area_name = study_area_name
        self.path_data = data_path
        self._transport_modes = transport_modes
        self._date_mode = date_mode
        self._date = date
        self._build_shape_data = build_shape_data
        self._interpolation_threshold = interpolation_threshold
        self._multiprocess = multiprocess
        self.run()

    def run(self):

        self._prepare_inputs()
        self._build_stops_data()
        self._build_path()

    def _prepare_inputs(self):
        self._stop_times_data = StopsTimes(self).data
        self._stops_data = Stops(self).data
        self._trips_data = Trips(self).data

        if not self._build_shape_data:
            self._shapes_data = Shapes(self).data
        elif self._build_shape_data:
            self._compute_shapes_txt()
            self._shapes_data = Shapes(self, input_file=self.__SHAPES_FILE_CREATED_NAME).data
            self._trips_data = Trips(self, input_file=self.__TRIPS_FILE_UPDATED_NAME).data

        if self._date_mode == "calendar_dates":
            self._calendar_dates_data = CalendarDates(self).data
        elif self._date_mode == "calendar":
            self._calendar_data = Calendar(self).data
        self._routes_data = Routes(self, transport_modes=self._transport_modes).data

    def _compute_shapes_txt(self):
        self.logger.info("Shapes computing...")
        stop_times_data = self._stop_times_data.sort_values(by=["trip_id", "stop_sequence"])
        stop_times_data = stop_times_data.merge(self._stops_data[["stop_id", "geometry"]], left_on='stop_id', right_on='stop_id').sort_values(["trip_id", "stop_sequence"])
        stop_ids_from_trip_id = stop_times_data.groupby("trip_id").agg(
            stop_id=("stop_id", list),
            stop_sequence=("stop_sequence", list),
            geometry=("geometry", list)
        ).reset_index()

        # create a shape_id regarding stop_id values
        stop_ids_from_trip_id = stop_ids_from_trip_id.assign(
            shape_id=[hashlib.sha256('_'.join(map(str, element)).encode('utf-8')).hexdigest() for element in stop_ids_from_trip_id["stop_id"]]
        )

        # update trips with shape_id features computed
        trips = Trips(self).data
        if "shape_id" in trips.columns:
            trips.drop(columns=["shape_id"], inplace=True)
        trips_updated = trips.merge(stop_ids_from_trip_id[["trip_id", "shape_id"]], on="trip_id")
        trips_updated.to_csv(os.path.join(self.path_data, self.__TRIPS_FILE_UPDATED_NAME), index=False)


        # create a new id to remove similar line when grouping on it
        stop_ids_from_trip_id_grouped_by_similar = stop_ids_from_trip_id.groupby("shape_id").agg(
            stop_id=("stop_id", "first"),
            stop_sequence=("stop_sequence", "first"),
            geometry=("geometry", "first")
        ).reset_index()

        stop_ids_from_trip_id_grouped_by_similar["shape_dist_traveled"] = [
            [0] + list(accumulate(list(
                map(
                    lambda pair: self.compute_wg84_line_length(LineString(pair)), list(zip(row, row[1:]))
                )
            ), operator.add))
            for row in stop_ids_from_trip_id_grouped_by_similar["geometry"]
        ]

        stop_ids_from_trip_id_exploded = stop_ids_from_trip_id_grouped_by_similar.explode(["stop_sequence", "geometry", "shape_dist_traveled"]).reset_index(drop=True)
        stop_ids_from_trip_id_exploded = stop_ids_from_trip_id_exploded.assign(
            shape_pt_lon=[geom.x for geom in stop_ids_from_trip_id_exploded["geometry"]],
            shape_pt_lat=[geom.y for geom in stop_ids_from_trip_id_exploded["geometry"]]
        )

        shape_data = stop_ids_from_trip_id_exploded.drop(columns=["geometry"])
        shape_data = shape_data.rename(columns={"stop_sequence": "shape_pt_sequence"})

        shape_data[
            ["shape_id", "shape_pt_lon", "shape_pt_lat", "shape_pt_sequence", "shape_dist_traveled"]
        ].to_csv(os.path.join(self.path_data, self.__SHAPES_FILE_CREATED_NAME), index=False)

    def _build_stops_data(self):
        self._stop_times_data.set_index("stop_id", inplace=True)
        self._stops_data.set_index("stop_id", inplace=True)
        stops_data = self._stops_data.join(self._stop_times_data).copy()
        stops_data.reset_index(inplace=True)

        stops_data.set_index("trip_id", inplace=True)
        #TODO filter with service_id found with tips
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
        self._temp_interpolated_points_cache = {}

        #TODO run at the start and filter stop as soon as possible
        if self._date_mode == "calendar_dates":
            service_id_selected = self._calendar_dates_data.loc[self._calendar_dates_data['date'] == self._date]
            # initializing interpolated points cache

            service_id_selected = service_id_selected.groupby("date").agg({
                "date": "first",
                "service_id": lambda x: list(set(list(x)))
            })
            service_id_selected = service_id_selected.to_dict("records")


        if self._date_mode == "calendar":
            date = datetime.datetime.strptime(self._date, '%Y%m%d')

            data = self._calendar_data.loc[
                (self._calendar_data[date.strftime("%A").lower()] == "1")
                & (
                    (self._calendar_data["start_date"] <= date) & (self._calendar_data["end_date"] >= date)
                )
            ][["start_date", "service_id"]]
            data = data.rename({'start_date': 'date'}, axis=1)

            service_id_selected = data.groupby("date").agg({
                "date": "first",
                "service_id": lambda x: list(set(list(x)))
            })
            service_id_selected["date"] = [row.strftime("%Y%m%d") for row in service_id_selected["date"]]
            service_id_selected = service_id_selected.to_dict("records")


        # EACH DAY
        for service in service_id_selected:

            stops_on_day = self._stops_data_build.loc[self._stops_data_build["service_id"].isin(service["service_id"])].copy()
            lines_on_day = self._shapes_data.loc[self._shapes_data["shape_id"].isin(stops_on_day["shape_id"].to_list())].copy()

            self.logger.info(f">>> WORKING DAY - {service['date']} - {lines_on_day.shape[0]} line(s) day found")
            date = datetime.datetime.strptime(service["date"], '%Y%m%d')
            self.compute_moving_geom(stops_on_day, lines_on_day, date)
            self.compute_fixed_geom(stops_on_day, lines_on_day)

            return

    def compute_moving_geom(self, stops_on_day, lines_on_day, date):

        stops_on_day = stops_on_day.assign(
            arrival_time=[self._compute_date(date, row) for row in stops_on_day["arrival_time"]],
            departure_time=[self._compute_date(date, row) for row in stops_on_day["departure_time"]],
            geometry=[self._compute_geom_precision(row, self.__COORDS_PRECISION) for row in stops_on_day["geometry"]],
        )
        stops_on_day["x"] = stops_on_day["geometry"].x
        stops_on_day["y"] = stops_on_day["geometry"].y
        stops_on_day = stops_on_day.rename({'arrival_time': 'start_date', 'departure_time': 'end_date'}, axis=1)

        if self._multiprocess:
            processes = [
                [self.compute_line, line, stops_on_day, date]
                for line in lines_on_day.to_dict('records')
            ]
            data_completed = run_process(processes)
        else:
            data_completed = [
                self.compute_line(line, stops_on_day, date)
                for line in lines_on_day.to_dict('records')
            ]

        self.logger.info(f"{len(data_completed)}")
        data_completed = list(filter(lambda x: not isinstance(x, list), data_completed))
        data_completed = pd.concat(data_completed)

        data_completed["x"] = data_completed.geometry.x
        data_completed["y"] = data_completed.geometry.y


        data_sp = GeoDataFrame(data_completed)
        data_sp = data_sp[self.__MOVING_DATA_COLUMNS].sort_values("start_date")
        # data = DfOptimizer(data).data

        data_sp["start_date"] = [int(row.timestamp()) for row in data_sp["start_date"]]
        data_sp["end_date"] = [int(row.timestamp()) for row in data_sp["end_date"]]

        data_sp = data_sp.astype({
            "start_date": "float",
            "end_date": "float",
            "x": "float",
            "y": "float",
            "stop_name": "category",
            "stop_code": "category",
            "route_type": "category",
            "route_long_name": "category",
            "route_short_name": "category",
            "direction_id": "category",
        })

        data_sp.to_parquet(f"{self._study_area_name}_{self.__MOVING_STOPS_OUTPUT_PARQUET_FILE}", compression='gzip')

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
        data_sp.to_parquet(f"{self._study_area_name}_{self.__BASE_STOPS_OUTPUT_PARQUET_FILE}", compression='gzip')

        #TODO improve it....
        lines = stops_data_copy[
            ["shape_id", "route_desc", "route_type", "route_short_name", "direction_id", "route_color", "route_text_color"]
        ].merge(
            lines_data, on="shape_id", how="left"
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
        data_sp.to_parquet(f"{self._study_area_name}_{self.__BASE_LINES_OUTPUT_PARQUET_FILE}", compression='gzip')

    def compute_line(self, line, stops_on_day, date):
        try:

            input_line_id = line["shape_id"]

            line_stops = self._get_stops_line(input_line_id, stops_on_day)

            trips_to_proceed = set(line_stops["trip_id"].to_list())

            trip_stops_computed = [
                self.compute_trip(date, line, line_stops, trip_id)
                for trip_id in trips_to_proceed
            ]

            # processes = [
            #     [self.compute_trip, date, line, line_stops, trip_id]
            #     for trip_id in trips_to_proceed
            # ]
            # trip_stops_computed = run_thread(processes)

        except ShapeIdError:
            return []

        # return itertools.chain(*trip_stops_computed)
        return pd.concat(trip_stops_computed)


    def compute_trip(self, date, line, line_stops, trip_id):
        trip_stops = line_stops.loc[line_stops["trip_id"] == trip_id]
        trip_stops_computed = self._build_interpolation_stops_on_trip(date, trip_id, trip_stops, line)

        return trip_stops_computed

    def _get_stops_line(self, input_line_id, stops_on_day):

        stops_line = stops_on_day.loc[stops_on_day["shape_id"] == input_line_id].copy()

        line_caracteristics = np.unique(stops_line[["route_type", "route_short_name"]].values)
        # TODO remove this exception
        if len(line_caracteristics) > 2:
            raise ShapeIdError(
                f"line name proceed should be unique (count: {len(line_caracteristics)} ; {','.join(line_caracteristics)}")
        caract_1, caract_2 = line_caracteristics
        self.logger.info(f"> Working line {input_line_id} ({caract_1}: {caract_2})")
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

        trip_stops["pos"] = np.arange(len(trip_stops))

        trip_stops_elements_full_list = []
        trip_stops_elements_full_list.append(trip_stops)

        trip_stops_elements = trip_stops.to_dict('records')
        stop_pairs = list(zip(trip_stops_elements, trip_stops_elements[1:]))

        # compute cached nodes
        pairs_already_computed = [
            (pair, self._temp_interpolated_points_cache[f"{pair[0]['stop_code']}_{pair[-1]['stop_code']}"])
            for pair in stop_pairs
            if f"{pair[0]['stop_code']}_{pair[-1]['stop_code']}" in self._temp_interpolated_points_cache
        ]
        trip_stops_elements_full_list.extend([
            self._compute_cached_nodes(pair, cache_gdf)
            for pair, cache_gdf in pairs_already_computed
        ])

        # compute new nodes
        pairs_to_compute = filter(lambda x: f"{x[0]['stop_code']}_{x[-1]['stop_code']}" not in self._temp_interpolated_points_cache, stop_pairs)
        trip_stops_elements_full_list.extend([
            self._compute_new_nodes(pair, line_geom_remaining)
            for pair in pairs_to_compute
        ])

        # finalize
        trip_data = pd.concat(trip_stops_elements_full_list)
        for column in trip_data.columns:
            if column not in ["start_date", "end_date", "pos", "geometry"]:
                trip_data.loc[:, column] = trip_data[column].unique()[0]

        return trip_data

    def _compute_new_nodes(self, pair, line_geom_remaining):
        first_stop = pair[0]
        next_stop = pair[-1]

        start_date = first_stop["end_date"]
        end_date = next_stop["start_date"]

        line_stop_geom, _ = self._get_dedicated_line_from_stop(first_stop["geometry"], next_stop["geometry"],
                                                               line_geom_remaining)

        # no need the first and the last to avoid duplicates
        interpolation_value = int(
            self.compute_wg84_line_length(line_geom_remaining) / self._interpolation_threshold)  # create func
        if interpolation_value == 0:
            interpolation_value = 1

        interpolated_points = tuple(
            line_stop_geom.interpolate(value, normalized=True)
            for value in np.linspace(0, 1, interpolation_value)
        )

        data = gpd.GeoDataFrame({
            "pos": map(lambda x: first_stop['pos'] + x / 100, range(0, len(interpolated_points))),
            'geometry': map(lambda x: self._compute_geom_precision(x, self.__COORDS_PRECISION), interpolated_points),
        })

        object_id = f"{first_stop['stop_code']}_{next_stop['stop_code']}"
        self._temp_interpolated_points_cache[object_id] = data

        intermediate_node_gdf = self.__compute_intermediates_daterange_nodes(data, start_date, end_date)

        return intermediate_node_gdf

    def _compute_cached_nodes(self, pair, cache_gdf):
        start_date = pair[0]["end_date"]
        end_date = pair[-1]["start_date"]

        intermediate_node_gdf = self.__compute_intermediates_daterange_nodes(cache_gdf, start_date, end_date)
        return intermediate_node_gdf

    def __compute_intermediates_daterange_nodes(self, input_gdf, start_date, end_date):
        gdf_found = input_gdf.copy(deep=True)
        # compute stop dates
        interpolation_value = gdf_found.shape[0] + 1  # TODO check this... +1... needed for pd.date_range
        interpolated_datetime = pd.date_range(start_date, end_date, periods=interpolation_value).to_list()
        interpolated_datetime_pairs = list(zip(interpolated_datetime, interpolated_datetime[1:]))
        gdf_found["start_date"] = list(map(lambda x: x[0], interpolated_datetime_pairs))
        gdf_found["end_date"] = list(map(lambda x: x[-1], interpolated_datetime_pairs))

        return gdf_found

    def _get_dedicated_line_from_stop(self, first_stop_geom, next_stop_geom, line_shape_geom_remained):

        projected_first_point = line_shape_geom_remained.interpolate(line_shape_geom_remained.project(first_stop_geom))
        projected_next_point = line_shape_geom_remained.interpolate(line_shape_geom_remained.project(next_stop_geom))

        all_points_coords = chain(line_shape_geom_remained.coords, projected_first_point.coords, projected_next_point.coords)
        all_points = map(Point, all_points_coords)
        new_line = LineString(sorted(all_points, key=line_shape_geom_remained.project))

        line_splitted_result = split(new_line, projected_first_point)
        new_line = line_splitted_result.geoms[-1]

        line_splitted_result = split(new_line, projected_next_point)
        stop_segment = line_splitted_result.geoms[0]
        line_geom_remaining = line_splitted_result.geoms[-1]

        # if line_geom_remaining.equals(stop_segment):
        #     stop_segment = LineString([projected_point, projected_point])

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



