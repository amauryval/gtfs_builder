
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

from datetime import datetime
from datetime import timedelta



class GtfsFormater:

    __SUB_STOPS_RESOLUTION = 10

    __DEFAULT_DATETIME = None
    __DEFAULT_END_STOP_CODE = None
    __DEFAULT_END_STOP_NAME = None
    __DEFAULT_STOP_NAME = None
    __DEFAULT_STOP_TYPE = None

    _OUTPUT_STOPS_POINTS = []

    def __init__(self):
        self.run()

    def run(self):
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

        # filter by a day TODO get date
        service_id_working = self._calendar_data.loc[self._calendar_data["monday"] == "1"]["service_id"].to_list()
        stops_data_build_filter_by_a_day = self._stops_data_build.loc[self._stops_data_build["service_id"].isin(service_id_working)].copy(deep=True)
        shapes_data_concerned_stops_data = self._shapes_data.loc[self._shapes_data["shape_id"].isin(stops_data_build_filter_by_a_day["shape_id"].to_list())].copy(deep=True)

        for line in shapes_data_concerned_stops_data.itertuples():
            print(f"line {line.shape_id}")

            line_stops = stops_data_build_filter_by_a_day.loc[stops_data_build_filter_by_a_day["shape_id"] == line.shape_id].copy(deep=True)
            # we need only unique stop from one trip
            # line_stops.drop_duplicates(subset="stop_name", inplace=True)
            line_stops.sort_values(by=["trip_id", "stop_sequence"], inplace=True)


            for trip_id in line_stops["trip_id"].to_list():

                print(f"trip {trip_id}")
                trip_stops = line_stops.loc[line_stops["trip_id"] == trip_id]

                line_geom_remaining = line.geometry
                start_stops = None

                for stop_position, stop in enumerate(trip_stops.itertuples()):

                    line_stop_geom, line_geom_remaining = self._get_dedicated_line_from_stop(stop, line_geom_remaining)
                    start_point = Point(line_stop_geom.coords[0])
                    end_point = Point(line_stop_geom.coords[-1])
                    arrival_time = datetime.strptime(stop.arrival_time, "%H:%M:%S")

                    start_stops = {
                        "day": "",
                        "date_time": arrival_time - timedelta(minutes=1) if start_stops is None else start_stops["date_time"],
                        "stop_code": self.__DEFAULT_END_STOP_CODE if start_stops is None else start_stops["stop_code"],
                        "geom": start_point if start_stops is None else start_stops["geom"], #projected_point.wkt,
                        "stop_name": self.__DEFAULT_END_STOP_NAME if start_stops is None else start_stops["stop_name"],
                        "stop_type": stop.route_type,
                        "line_name": stop.route_long_name,
                        "line_name_short": stop.route_short_name,
                        "direction_id": stop.direction_id,
                        "trip_id": trip_id,
                        "pos": stop_position
                    }
                    last_stops = {
                        "day": "",
                        "date_time": arrival_time,
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
                    self._OUTPUT_STOPS_POINTS.append(start_stops)

                    start_date = start_stops['date_time']
                    end_date = last_stops['date_time']

                    interpolation_value = int(line_stop_geom.length / self.__SUB_STOPS_RESOLUTION)
                    interpolated_datetime = pd.date_range(start_date, end_date, periods=interpolation_value).to_list()[1:-1]
                    interpolated_points = tuple(
                        line_stop_geom.interpolate(value, normalized=True)
                        for value in np.linspace(0, 1, interpolation_value)
                    )[1:-1]
                    interpolated_data = zip(interpolated_datetime, interpolated_points)

                    # about after the last stop, do not process it...
                    if stop_position != trip_stops.shape[0] - 1:
                        for sub_stop_position, (date_time, point) in enumerate(interpolated_data):
                            self._OUTPUT_STOPS_POINTS.append({
                                "day": "",
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
                            })

                        start_stops = last_stops

                data = pd.DataFrame(self._OUTPUT_STOPS_POINTS).sort_values(by=["date_time"], ascending=True)
                # data.drop_duplicates(subset=["date_time"], inplace=True)

                import geopandas
                gdf = geopandas.GeoDataFrame(data, geometry=data["geom"])
                gdf.drop(columns=["geom"], inplace=True)
                gdf.to_file("ahaha5.geojson", driver="GeoJSON")
                for f in data["geom"].to_list(): print(f)


    def _get_dedicated_line_from_stop(self, stop, line_shape_geom_remained):
        projected_point = line_shape_geom_remained.interpolate(line_shape_geom_remained.project(stop.geometry))

        projected_point_buffered = projected_point.buffer(0.1)
        line_splitted = split(line_shape_geom_remained, projected_point_buffered)
        line_splitted_result = list(line_splitted.geoms)
        first_seg = line_splitted_result[0]
        second_seg = line_splitted_result[-1]

        # create the new segment
        stop_segment_coords = list(first_seg.coords)
        stop_segment_coords.append(list(projected_point.coords)[0])
        stop_segment = LineString(stop_segment_coords)

        # update the line geom remaining
        line_geom_remaining = list(second_seg.coords)
        line_geom_remaining.insert(0, list(projected_point.coords)[0])
        line_geom_remaining = LineString(line_geom_remaining)

        return stop_segment, line_geom_remaining

if __name__ == '__main__':
    GtfsFormater()
