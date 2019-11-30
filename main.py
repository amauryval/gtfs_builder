from itertools import chain

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
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from concurrent.futures import as_completed

import geopandas




def run_threads(processes, workers_number=4):

    with ThreadPoolExecutor(max_workers=workers_number) as executor:

        executions = []
        for process in processes:
            if isinstance(process, list):
                executions.append(executor.submit(*process))
            else:
                executions.append(executor.submit(process))
        # to return exceptions
        for exe in as_completed(executions):
            exe._Future__get_result()



class GtfsFormater:

    __SUB_STOPS_RESOLUTION = 1000

    __DAYS_MAPPING = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ]

    __DEFAULT_DATETIME = None
    __DEFAULT_END_STOP_CODE = None
    __DEFAULT_END_STOP_NAME = None
    __DEFAULT_STOP_NAME = None
    __DEFAULT_STOP_TYPE = None

    _OUTPUT_STOPS_POINTS = []

    def __init__(self):
        self.run()

    def run(self):
        print(datetime.datetime.now())
        self._prepare_inputs()
        self._build_stops_data()
        self._build_path()

    def _prepare_inputs(self):
        self._shapes_data = Shapes().data
        self._stops_data = Stops().data
        self._stop_times_data = StopsTimes().data
        self._trips_data = Trips().data
        self._calendar_data = Calendar().data
        self._routes_data = Routes().data

    def _build_stops_data(self):
        self._stop_times_data.set_index("stop_id", inplace=True)
        self._stops_data.set_index("stop_id", inplace=True)
        stops_data = self._stops_data.join(self._stop_times_data).copy(deep=True)
        stops_data.reset_index(inplace=True)

        stops_data.set_index("trip_id", inplace=True)
        self._trips_data.set_index("trip_id", inplace=True)
        stops_trips_data = stops_data.join(self._trips_data).copy(deep=True)
        stops_trips_data.reset_index(inplace=True)

        stops_trips_data.set_index("route_id", inplace=True)
        self._routes_data.set_index("route_id", inplace=True)
        stops_trips_routes_data_complete = stops_trips_data.join(self._routes_data).copy(deep=True)
        stops_trips_routes_data_complete.reset_index(inplace=True)

        data_otptimized = DfOptimizer(stops_trips_routes_data_complete)
        print(data_otptimized.memory_usage)
        self._stops_data_build = data_otptimized.data

    def _build_path(self):

        end_date = max(self._calendar_data["end_date"].to_list())
        start_date = min(self._calendar_data["start_date"].to_list())

        service_ids_to_proceed_by_day = {}
        while start_date <= end_date:
            start_date_day = self.__DAYS_MAPPING[start_date.weekday()]
            service_id_working = self._calendar_data.loc[
                (self._calendar_data[start_date_day] == "1") & ((self._calendar_data["start_date"] >= start_date) | (self._calendar_data["end_date"] <= start_date))
            ]["service_id"].to_list()

            service_ids_to_proceed_by_day[start_date] = service_id_working

            start_date += datetime.timedelta(days=1)

        for date, service_ids_working in service_ids_to_proceed_by_day.items():
            print(f">>>> {date}")

            # EACH DAY
            stops_data_build_filter_by_a_day = self._stops_data_build.loc[self._stops_data_build["service_id"].isin(service_ids_working)].copy()
            shapes_data_concerned_stops_data = self._shapes_data.loc[self._shapes_data["shape_id"].isin(stops_data_build_filter_by_a_day["shape_id"].to_list())].copy()

            processes = []
            for line in shapes_data_concerned_stops_data.itertuples():
                if line.shape_id is not None:# "4503603929114738":
                    self._run_each_stop_from_each_line(date, stops_data_build_filter_by_a_day, line)
                    # processes.append([self._run_each_stop_from_each_line, date, stops_data_build_filter_by_a_day, line])

            # run_threads(processes, 8)

                    data = pd.concat(self._OUTPUT_STOPS_POINTS, ignore_index=True).sort_values(by=["date_time"], ascending=True)
                    gdf = geopandas.GeoDataFrame(data, geometry=data["geom"])
                    gdf.drop(columns=["geom"], inplace=True)
                    gdf.to_file(f"data3.gpkg", driver="GPKG", layer=f"{line.shape_id}")
                    print(datetime.datetime.now())
            assert False


    def _run_each_stop_from_each_line(self, date, stops_data_build_filter_by_a_day, line):

        objects_line = {}
        line_stops_computed = list()
        print(f"line {line.shape_id}")

        line_stops = stops_data_build_filter_by_a_day.loc[stops_data_build_filter_by_a_day["shape_id"] == line.shape_id].copy()
        line_stops.sort_values(by=["trip_id", "stop_sequence"], inplace=True)

        for trip_id in set(line_stops["trip_id"].to_list()):

            # print("trip", trip_id)
            trip_stops = line_stops.loc[line_stops["trip_id"] == trip_id]

            line_geom_remaining = line.geometry
            start_stops = None

            for stop_position, stop in enumerate(trip_stops.itertuples()):

                start_stop_code = self.__DEFAULT_END_STOP_CODE if start_stops is None else start_stops["stop_code"]

                hours, minutes, seconds = map(int, stop.arrival_time.split(":"))
                arrival_time = date + datetime.timedelta(seconds=seconds, minutes=minutes, hours=hours)
                start_date = arrival_time - datetime.timedelta(minutes=1) if start_stops is None else start_stops["date_time"]
                end_date = arrival_time

                is_data_found = objects_line.get(f"{start_stop_code}_{stop.stop_code}", None)
                if is_data_found is not None:
                    line_stop_geom, interpolated_points = is_data_found
                else:
                    line_stop_geom, line_geom_remaining = self._get_dedicated_line_from_stop(stop, line_geom_remaining)

                    # data interpolated
                    interpolation_value = int(line_stop_geom.length / self.__SUB_STOPS_RESOLUTION) #create func
                    interpolated_points = tuple(
                        line_stop_geom.interpolate(value, normalized=True)
                        for value in np.linspace(0, 1, interpolation_value)
                    )[1:-1]

                    objects_line[f"{start_stop_code}_{stop.stop_code}"] = (line_stop_geom, interpolated_points)

                start_point = Point(line_stop_geom.coords[0])
                end_point = Point(line_stop_geom.coords[-1])

                start_stops = {
                    "date_time": start_date,
                    "stop_code": start_stop_code,
                    "geom": start_point if start_stops is None else start_stops["geom"],
                    "stop_name": self.__DEFAULT_END_STOP_NAME if start_stops is None else start_stops["stop_name"],
                    "stop_type": stop.route_type,
                    "line_name": stop.route_long_name,
                    "line_name_short": stop.route_short_name,
                    "direction_id": stop.direction_id,
                    "trip_id": trip_id,
                    "pos": stop_position
                }
                last_stops = {
                    "date_time": end_date,
                    "stop_code": stop.stop_code,
                    "geom": end_point,
                    "stop_name": stop.stop_name,
                    "stop_type": stop.route_type,
                    "line_name": stop.route_long_name,
                    "line_name_short": stop.route_short_name,
                    "direction_id": stop.direction_id,
                    "trip_id": trip_id,
                    "pos": stop_position + 1
                }
                line_stops_computed.append(start_stops)
                # print(start_stops["geom"].wkt)
                # start_date = start_stops['date_time']
                # end_date = last_stops['date_time']

                interpolation_value = int(line_stop_geom.length / self.__SUB_STOPS_RESOLUTION)
                interpolated_datetime = pd.date_range(start_date, end_date, periods=interpolation_value).to_list()[1:-1]
                # interpolated_points = tuple(
                #     line_stop_geom.interpolate(value, normalized=True)
                #     for value in np.linspace(0, 1, interpolation_value)
                # )[1:-1]
                interpolated_data = zip(interpolated_datetime, interpolated_points)

                # about after the last stop, do not process it...
                if stop_position != trip_stops.shape[0] - 1:
                    for sub_stop_position, (date_time, point) in enumerate(interpolated_data):
                        new_point = {
                            "date_time": date_time,
                            "stop_code": None,
                            "geom": point,
                            "stop_name": None,
                            "stop_type": stop.route_type,
                            "line_name": stop.route_long_name,
                            "line_name_short": stop.route_short_name,
                            "direction_id": stop.direction_id,
                            "trip_id": trip_id,
                            "pos": f"{stop_position}.{sub_stop_position}"
                        }
                        line_stops_computed.append(new_point)
                        # print(point.wkt)
                    start_stops = last_stops

        trip_done = pd.DataFrame(line_stops_computed)
        self._OUTPUT_STOPS_POINTS.append(trip_done)
        print(len(self._OUTPUT_STOPS_POINTS))

    def _get_dedicated_line_from_stop(self, stop, line_shape_geom_remained):

        # buffer_value_to_split = 1
        # num_retries = 4
        #
        projected_point = line_shape_geom_remained.interpolate(line_shape_geom_remained.project(stop.geometry))
        # projected_point_buffered = projected_point.buffer(buffer_value_to_split)
        #
        # line_splitted = None
        # for _ in range(0, num_retries):
        #     try:
        #         line_splitted = split(line_shape_geom_remained, projected_point_buffered)
        #     except TypeError:
        #         buffer_value_to_split = buffer_value_to_split / 10
        #         projected_point_buffered = projected_point.buffer(buffer_value_to_split)
        #
        # line_splitted_result = list(line_splitted.geoms)
        # first_seg = line_splitted_result[0]
        # second_seg = line_splitted_result[-1]
        #
        # # create the new segment
        # stop_segment_coords = list(first_seg.coords)
        # stop_segment_coords.append(list(projected_point.coords)[0])
        # stop_segment = LineString(stop_segment_coords)
        #
        # # update the line geom remaining
        # line_geom_remaining = list(second_seg.coords)
        # line_geom_remaining.insert(0, list(projected_point.coords)[0])
        # line_geom_remaining = LineString(line_geom_remaining)

        all_points_coords = chain(line_shape_geom_remained.coords, projected_point.coords)
        all_points = map(Point, all_points_coords)
        new_line = LineString(sorted(all_points, key=line_shape_geom_remained.project))
        line_splitted_result = split(new_line, projected_point.coords)
        stop_segment = line_splitted_result[0]
        line_geom_remaining = line_splitted_result[-1]


        return stop_segment, line_geom_remaining

    def _get_weekday_from_date(self, date_value, day):

        while date_value.weekday() != self.__DAYS_MAPPING[day]:
            date_value += datetime.timedelta(days=1)

        return date_value

if __name__ == '__main__':
    GtfsFormater()
