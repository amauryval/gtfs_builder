
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


class GtfsFormater:

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


        # filter by a day
        service_id_working = self._calendar_data.loc[self._calendar_data["monday"] == "1"]["service_id"].to_list()
        stops_data_build_filter_by_a_day = self._stops_data_build.loc[self._stops_data_build["service_id"].isin(service_id_working)].copy(deep=True)

        shapes_data_concerned_stops_data = self._shapes_data.loc[self._shapes_data["shape_id"].isin(stops_data_build_filter_by_a_day["shape_id"].to_list())].copy(deep=True)

        count_line = 1
        count_trip = 1
        for line in shapes_data_concerned_stops_data.itertuples():
            print("line ", count_line)
            source_line_geom = line.geometry
            line_stops = stops_data_build_filter_by_a_day.loc[stops_data_build_filter_by_a_day["shape_id"] == line.shape_id].copy(deep=True)
            # we need only unique stop from one trip
            # line_stops.drop_duplicates(subset="stop_name", inplace=True)
            line_stops.sort_values(by=["trip_id", "stop_sequence"], inplace=True)

            for trip_id in line_stops["trip_id"].to_list():
                print("trip ", count_trip)
                trip_stops = line_stops.loc[line_stops["trip_id"] == trip_id]
                line_geom = source_line_geom
                for stop in trip_stops.itertuples():

                    projected_point = line_geom.interpolate(line_geom.project(stop.geometry))

                    projected_point_buffered = projected_point.buffer(0.000001)
                    line_splitted = split(line_geom, projected_point_buffered)
                    line_splitted_result = list(line_splitted.geoms)
                    first_seg = line_splitted_result[0]
                    second_seg = line_splitted_result[-1]

                    # create the new segment
                    new_segment_coords = list(first_seg.coords)
                    new_segment_coords.append(list(projected_point.coords)[0])
                    new_segment = LineString(new_segment_coords)

                    # update the line geom remaining
                    line_geom_remaining = list(second_seg.coords)
                    line_geom_remaining.insert(0, list(projected_point.coords)[0])
                    line_geom = LineString(line_geom_remaining)

                    new_geom = {
                        "day": "",
                        "start_time": stop.departure_time,
                        "end_time": stop.arrival_time,
                        "end_stop_code": stop.stop_code,
                        "end_stop_name": stop.stop_name,
                        "geom_start_stop": Point(new_segment.coords[0]).wkt,
                        "geom_path": new_segment.wkt,
                        "geom_end_stop": projected_point.wkt,
                        "stop_name": stop.stop_name,
                        "stop_type": stop.route_type,
                        "line_name": stop.route_long_name,
                        "line_name_short": stop.route_short_name,
                        "direction_id": stop.direction_id
                    }
                count_trip += 1
            count_line += 1


if __name__ == '__main__':
    GtfsFormater()
