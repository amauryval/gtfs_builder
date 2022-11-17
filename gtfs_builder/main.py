from typing import List, Tuple, Union
from typing import Dict
from typing import Optional

from pandas import Timestamp

from gtfs_builder.db.base import Base
from gtfs_builder.db.moving_points import MovingPoints
import re

import os
import datetime
import copy
from pyproj import Geod

import shapely
from shapely import wkt
from geospatial_lib import GeoSpatialLib
from geospatial_lib.misc.processing import method_processing_modes
from itertools import chain
import numpy as np
import geopandas as gpd

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

from itertools import accumulate
import operator

from spatialpandas import GeoDataFrame


class ShapeIdError(Exception):
    pass


def str_to_dict_from_regex(string_value, regex):
    pattern = re.compile(regex)
    extraction = pattern.match(string_value)
    return extraction.groupdict()


class GtfsRebuilder(GeoSpatialLib):
    pd.options.mode.chained_assignment = None

    __MAIN_DB_SCHEMA = "gtfs_data"
    __PG_EXTENSIONS = ["btree_gist", "postgis"]

    __COORDINATES_PRECISION = 3

    __RAW_DATA_DIR = "../input_data"

    __SHAPES_FILE_CREATED_NAME = "shapes_computed.txt"
    __TRIPS_FILE_UPDATED_NAME = "trips_updated.txt"

    __SUB_STOPS_RESOLUTION = 200  # 1pt for each 25 meters
    __DAYS_MAPPING = (
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    )

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
    ]

    __MOVING_STOPS_OUTPUT_PARQUET_FILE = "moving_stops.parq"
    __BASE_STOPS_OUTPUT_PARQUET_FILE = "base_stops_data.parq"
    __BASE_LINES_OUTPUT_PARQUET_FILE = "base_lines_data.parq"

    def __init__(self,
                 study_area_name: str,
                 data_path: str,
                 transport_modes: Optional[List[str]] = None,
                 date_mode: str = "calendar",
                 date: str = None,
                 build_shape_data: bool = False,
                 interpolation_threshold: int = 1000,
                 multiprocess: bool = False,
                 output_format: str = "file",
                 db_mode: str = "append"
                 ):
        super().__init__()

        self._output_format = output_format
        self._db_mode = db_mode

        self._study_area_name = study_area_name
        self.path_data = data_path
        self._transport_modes = transport_modes
        self._date_mode = date_mode
        self._date = date
        self._build_shape_data = build_shape_data
        self._interpolation_threshold = interpolation_threshold
        self._multiprocess = multiprocess
        self.run()

    def run(self) -> None:
        self.logger.info(f"Computing {self._study_area_name} GTFS...")
        self._prepare_inputs()
        self._build_stops_data()
        self._build_path()

    def _prepare_db(self) -> None:
        self.logger.info('Prepare database')

        self._credentials = {
            **str_to_dict_from_regex(
                os.environ["ADMIN_DB_URL"],
                r".+:\/\/(?P<username>.+):(?P<password>.+)@(?P<host>[\W\w-]+):(?P<port>\d+)\/(?P<database>.+)"
            ),
        }

        db_sessions = self.init_db(
            **self._credentials,
            extensions=self.__PG_EXTENSIONS,
            overwrite=False
        )
        self._engine = db_sessions["engine"]
        schemas = Base.metadata._schemas
        for schema in schemas:
            self.init_schema(self._engine, schema)
        if self._db_mode == "overwrite":  # drop tables
            Base.metadata.drop_all(self._engine)
            Base.metadata.create_all(self._engine)

        tables = [table.fullname for table in Base.metadata.sorted_tables]
        if len(tables) > 0:
            tables_str = ', '.join(tables)
            self.logger.info(f'({len(tables)}) tables  found: {tables_str}')
        else:
            raise ValueError("Not tables found on DB!")

    def _prepare_inputs(self) -> None:
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

    def _compute_shapes_txt(self) -> None:
        self.logger.info("Shapes computing...")
        stop_times_data = self._stop_times_data.sort_values(by=["trip_id", "stop_sequence"])
        stop_times_data = stop_times_data.merge(self._stops_data[["stop_id", "geometry"]], left_on='stop_id',
                                                right_on='stop_id').sort_values(["trip_id", "stop_sequence"])
        stop_ids_from_trip_id = stop_times_data.groupby("trip_id").agg(
            stop_id=("stop_id", list),
            stop_sequence=("stop_sequence", list),
            geometry=("geometry", list)
        ).reset_index()

        # create a shape_id regarding stop_id values
        stop_ids_from_trip_id = stop_ids_from_trip_id.assign(
            shape_id=[hashlib.sha256('_'.join(map(str, element)).encode('utf-8')).hexdigest() for element in
                      stop_ids_from_trip_id["stop_id"]]
        )

        # update trips with shape_id features computed
        # trips = Trips(self).data
        if "shape_id" in self._trips_data.columns:
            self._trips_data.drop(columns=["shape_id"], inplace=True)
        trips_updated = self._trips_data.merge(stop_ids_from_trip_id[["trip_id", "shape_id"]], on="trip_id")
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

        stop_ids_from_trip_id_exploded = stop_ids_from_trip_id_grouped_by_similar.explode(
            ["stop_sequence", "geometry", "shape_dist_traveled"]).reset_index(drop=True)
        stop_ids_from_trip_id_exploded = stop_ids_from_trip_id_exploded.assign(
            shape_pt_lon=[geom.x for geom in stop_ids_from_trip_id_exploded["geometry"]],
            shape_pt_lat=[geom.y for geom in stop_ids_from_trip_id_exploded["geometry"]]
        )

        shape_data = stop_ids_from_trip_id_exploded.drop(columns=["geometry"])
        shape_data = shape_data.rename(columns={"stop_sequence": "shape_pt_sequence"})

        shape_data[
            ["shape_id", "shape_pt_lon", "shape_pt_lat", "shape_pt_sequence", "shape_dist_traveled"]
        ].to_csv(os.path.join(self.path_data, self.__SHAPES_FILE_CREATED_NAME), index=False)

    def _build_stops_data(self) -> None:
        self._stop_times_data.set_index("stop_id", inplace=True)
        self._stops_data.set_index("stop_id", inplace=True)
        stops_data = self._stops_data.join(self._stop_times_data).copy()
        stops_data.reset_index(inplace=True)

        stops_data.set_index("trip_id", inplace=True)
        # TODO filter with service_id found with tips
        self._trips_data.set_index("trip_id", inplace=True)
        stops_trips_data = stops_data.join(self._trips_data).copy()
        stops_trips_data.reset_index(inplace=True)

        stops_trips_data.set_index("route_id", inplace=True)
        self._routes_data.set_index("route_id", inplace=True)
        stops_trips_routes_data_complete = stops_trips_data.merge(self._routes_data, on="route_id", how="right").copy()
        stops_trips_routes_data_complete.reset_index(inplace=True)

        self._stops_data_build = stops_trips_routes_data_complete

    def _build_path(self) -> None:
        self.logger.info("Go to path building")
        self._CACHE_DATA = {}

        date = datetime.datetime.strptime(self._date, '%Y%m%d')

        # TODO run at the start and filter stop as soon as possible
        if self._date_mode == "calendar_dates":

            service_id_selected = self._calendar_dates_data.loc[self._calendar_dates_data['date'] == date]
            # initializing interpolated points cache

            service_id_selected = service_id_selected.groupby("date").agg({
                "date": "first",
                "service_id": lambda x: list(set(list(x)))
            })

        elif self._date_mode == "calendar":

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
        else:
            raise ValueError("service_id not defined")

        service_id_selected["date"] = [row.strftime("%Y%m%d") for row in service_id_selected["date"]]
        service_id_selected = service_id_selected.to_dict("records")

        # EACH DAY (means a service...)
        for service in service_id_selected:
            stops_on_day = self._stops_data_build.loc[
                self._stops_data_build["service_id"].isin(service["service_id"])].copy()
            lines_on_day = self._shapes_data.loc[
                self._shapes_data["shape_id"].isin(stops_on_day["shape_id"].to_list())].copy()

            self.logger.info(
                f">>> WORKING DAY - {service['date']} - {lines_on_day.shape[0]} line(s) for this day found")
            date = datetime.datetime.strptime(service["date"], '%Y%m%d')
            self.compute_moving_geom(stops_on_day, lines_on_day, date)
            self.compute_fixed_geom(stops_on_day, lines_on_day)

            return

    def compute_moving_geom(self, stops_on_day: gpd.GeoDataFrame, lines_on_day: gpd.GeoDataFrame,
                            date: datetime) -> None:
        stops_on_day["arrival_time"] = [self._compute_date(date, row) for row in stops_on_day["arrival_time"]]
        stops_on_day["departure_time"] = [self._compute_date(date, row) for row in stops_on_day["departure_time"]]
        stops_on_day["geometry"] = [self._compute_geom_precision(row, self.__COORDINATES_PRECISION) for row in
                                    stops_on_day["geometry"]]

        stops_on_day = stops_on_day.rename({'arrival_time': 'start_date', 'departure_time': 'end_date'}, axis=1)

        if self._multiprocess:
            processes = [
                [self.compute_line, line, stops_on_day]
                for line in lines_on_day.to_dict('records')
            ]
            data_completed = method_processing_modes(processes, mode="processing")
        else:
            data_completed = [
                self.compute_line(line, stops_on_day)
                for line in lines_on_day.to_dict('records')
            ]

        self.logger.info(f"{len(data_completed)}")
        data_completed = list(filter(lambda x: not isinstance(x, list), data_completed))
        data_completed = pd.concat(data_completed)

        if self._output_format == "db":
            self._prepare_db()
            data_completed["study_area"] = self._study_area_name
            input_data = self.gdf_design_checker(self._engine, self.__MAIN_DB_SCHEMA, MovingPoints.__table__.name,
                                                 data_completed, epsg=4326)
            dict_data = self.df_to_dicts_list(input_data, 4326)
            self.dict_list_to_db(self._engine, dict_data, self.__MAIN_DB_SCHEMA, MovingPoints.__table__.name)

        else:
            data_sp = GeoDataFrame(data_completed, geometry="geometry")
            data_sp = data_sp[self.__MOVING_DATA_COLUMNS].sort_values("start_date")

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
            })

            data_sp.to_parquet(f"{self._study_area_name}_{self.__MOVING_STOPS_OUTPUT_PARQUET_FILE}", compression='gzip')

    def compute_fixed_geom(self, stops_data: gpd.GeoDataFrame, lines_data: gpd.GeoDataFrame) -> None:
        stops_data_copy = stops_data.copy(deep=True)
        stops = stops_data_copy.groupby(["stop_code"], sort=False).agg({
            "stop_code": "first",
            "geometry": "first",
            "stop_name": lambda x: set(list(x)),
            "route_short_name": lambda x: set(list(x)),
            "route_desc": lambda x: set(list(x)),
            "route_type": lambda x: set(list(x)),
            "route_color": lambda x: set(list(x)),
            "route_text_color": lambda x: set(list(x)),
        }).dropna()
        stops_data = gpd.GeoDataFrame(stops)
        data_sp = GeoDataFrame(stops_data)
        data_sp.to_parquet(f"{self._study_area_name}_{self.__BASE_STOPS_OUTPUT_PARQUET_FILE}", compression='gzip')

        # TODO improve it....
        lines = stops_data_copy[
            ["shape_id", "route_desc", "route_type", "route_short_name", "direction_id", "route_color",
             "route_text_color"]
        ].merge(
            lines_data, on="shape_id", how="left"
        ).groupby(["shape_id"]).agg({
            "shape_id": "first",
            "geometry": "first",
            "route_desc": lambda x: set(list(x)),
            "route_type": lambda x: set(list(x)),
            "route_short_name": lambda x: set(list(x)),
            "direction_id": lambda x: set(list(x)),
            "route_color": lambda x: set(list(x)),
            "route_text_color": lambda x: set(list(x)),
        })
        lines_data = gpd.GeoDataFrame(lines)
        data_sp = GeoDataFrame(lines_data)
        data_sp.to_parquet(f"{self._study_area_name}_{self.__BASE_LINES_OUTPUT_PARQUET_FILE}", compression='gzip')

    def compute_line(self, line: Dict, stops_on_day: gpd.GeoDataFrame) -> Union[pd.DataFrame, List]:
        try:

            input_line_id = line["shape_id"]
            line_stops = self._get_stops_line(input_line_id, stops_on_day)

            trips_to_proceed = set(line_stops["trip_id"].to_list())

            processes = [
                [self.compute_trip, line, line_stops, trip_id]
                for trip_id in trips_to_proceed
            ]

            trip_stops_computed = method_processing_modes(processes, mode=None)

        except ShapeIdError:
            return []

        return pd.concat(trip_stops_computed)

    def compute_trip(self, line: Dict, line_stops: gpd.GeoDataFrame, trip_id: str) -> gpd.GeoDataFrame:
        trip_stops = line_stops.loc[line_stops["trip_id"] == trip_id]
        trip_stops_computed = self._build_interpolation_stops_on_trip(trip_stops, line)

        return trip_stops_computed

    def _get_stops_line(self, input_line_id: str, stops_on_day: gpd.GeoDataFrame) -> gpd.geodataframe:

        stops_line = stops_on_day.loc[stops_on_day["shape_id"] == input_line_id]

        line_attributes = pd.unique(
            stops_line[["route_type", "route_short_name", "route_long_name"]].values.ravel('K'))
        # TODO remove this exception, simplification can be done...
        self.logger.info(f"> Working line {input_line_id} ({', '.join(line_attributes)})")

        return stops_line.sort_values(by=["trip_id", "stop_sequence"])

    @staticmethod
    def compute_wg84_line_length(input_geom: LineString) -> float:
        """
        Compute the length of a wg84 line (LineString and MultiLineString)
        :param input_geom: input geometry
        :type input_geom: shapely.geometry.LineString or shapely.geometry.MultiLineString
        :return: the line length
        :rtype: float
        """

        line_length = Geod(ellps="WGS84").geometry_length(input_geom)

        return line_length

    @staticmethod
    def _compute_date(date_to_add: datetime, feature_date: str) -> datetime:
        hours, minutes, seconds = map(int, feature_date.split(":"))
        return date_to_add + datetime.timedelta(seconds=seconds, minutes=minutes, hours=hours)

    @staticmethod
    def _compute_geom_precision(feature_geom: Point, precision: int) -> Point:
        return wkt.loads(wkt.dumps(feature_geom, rounding_precision=precision))

    def _build_interpolation_stops_on_trip(self, trip_stops: gpd.GeoDataFrame, line: Dict) -> gpd.GeoDataFrame:

        line_geom_remaining = line["geometry"]

        trip_stops["pos"] = np.arange(len(trip_stops))

        trip_stops_elements = trip_stops.to_dict('records')
        stop_pairs = zip(trip_stops_elements, trip_stops_elements[1:])

        # compute new nodes
        for pair in stop_pairs:
            trip_stops_elements.extend(self._compute_new_nodes(pair, line_geom_remaining))
        # finalize
        trip_data = gpd.GeoDataFrame(trip_stops_elements).sort_values("pos")
        trip_data = trip_data.loc[trip_data["start_date"] != trip_data["end_date"]]
        trip_data["x"] = trip_data.geometry.x
        trip_data["y"] = trip_data.geometry.y
        trip_data["validity_range"] = [self._format_validity_range(*row) for row in
                                       zip(trip_data["start_date"], trip_data["end_date"])]
        return trip_data

    def _compute_new_nodes(self, pair: Tuple[Dict, Dict], line_geom_remaining: LineString) -> List[Dict]:
        first_stop, next_stop = pair

        start_date = first_stop["end_date"]
        end_date = next_stop["start_date"]

        stop_id_pairs = f"{first_stop['stop_id']}_{next_stop['stop_id']}"
        if stop_id_pairs not in self._CACHE_DATA:

            line_stop_geom, _ = self._get_dedicated_line_from_stop(
                first_stop["geometry"],
                next_stop["geometry"],
                line_geom_remaining
            )

            # no need the first and the last to avoid duplicates
            interpolation_value = int(
                self.compute_wg84_line_length(line_geom_remaining) / self._interpolation_threshold)  # create func

            interpolated_points = tuple(
                line_stop_geom.interpolate(value, normalized=True)
                for value in np.linspace(0, 1, interpolation_value)
            )

            interpolated_datetime = pd.date_range(start_date, end_date, periods=interpolation_value).to_list()
            interpolated_datetime_pairs = list(zip(interpolated_datetime, interpolated_datetime[1:]))

            data = [
                self._compute_node(first_stop, enum, point, dates)
                for enum, (point, dates) in enumerate(zip(interpolated_points, interpolated_datetime_pairs))
            ]

            self._CACHE_DATA[f"{first_stop['stop_id']}_{next_stop['stop_id']}"] = [feature["geometry"] for feature in
                                                                                   data]
            self._CACHE_DATA[f"{next_stop['stop_id']}_{first_stop['stop_id']}"] = [feature["geometry"] for feature in
                                                                                   data][::-1]

        else:
            interpolate_points_cache = self._CACHE_DATA[f"{first_stop['stop_id']}_{next_stop['stop_id']}"]

            interpolated_datetime = pd.date_range(start_date, end_date, periods=len(interpolate_points_cache)).to_list()
            interpolated_datetime_pairs = zip(interpolated_datetime, interpolated_datetime[1:])
            data = [
                self._compute_node(first_stop, enum, point, dates)
                for enum, (point, dates) in enumerate(zip(interpolate_points_cache, interpolated_datetime_pairs))
            ]

        return data

    def _compute_node(self, source_node: Dict, enum: int, point: Point, dates: Tuple) -> Dict:
        source_node_copy = copy.deepcopy(source_node)
        source_node_copy["pos"] = source_node['pos'] + enum / 100
        source_node_copy["geometry"] = self._compute_geom_precision(point, self.__COORDINATES_PRECISION)
        source_node_copy["start_date"] = dates[0]
        source_node_copy["end_date"] = dates[-1]
        return source_node_copy

    @staticmethod
    def _get_dedicated_line_from_stop(first_stop_geom: Point, next_stop_geom: Point,
                                      line_shape_geom_remained: LineString) -> Tuple[LineString, LineString]:

        projected_first_point = line_shape_geom_remained.interpolate(line_shape_geom_remained.project(first_stop_geom))
        projected_next_point = line_shape_geom_remained.interpolate(line_shape_geom_remained.project(next_stop_geom))

        all_points_coordinates = chain(line_shape_geom_remained.coords, projected_first_point.coords,
                                       projected_next_point.coords)
        all_points = map(Point, all_points_coordinates)
        new_line = LineString(sorted(all_points, key=line_shape_geom_remained.project))

        line_split = split(new_line, projected_first_point)
        new_line = line_split.geoms[-1]

        line_split = split(new_line, projected_next_point)
        stop_segment = line_split.geoms[0]
        line_geom_remaining = line_split.geoms[-1]

        return stop_segment, line_geom_remaining

    @staticmethod
    def _format_validity_range(start_date: Optional[Timestamp] = None,
                               end_date: Optional[Timestamp] = None) -> DateTimeRange:
        if start_date is None:
            start_date = datetime.date.min
        if end_date is None:
            end_date = datetime.date.max
        return DateTimeRange(start_date, end_date)
