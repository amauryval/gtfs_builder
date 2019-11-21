import pandas as pd

from gtfs_builder.gtfs_main.shapes_in import Shapes
from gtfs_builder.gtfs_main.stops_in import Stops
from gtfs_builder.gtfs_main.stop_times_in import StopsTimes
from gtfs_builder.gtfs_main.trips_in import Trips
from gtfs_builder.gtfs_main.calendar_in import Calendar


if __name__ == '__main__':
    shapes_data = Shapes().data
    stops_data = Stops().data
    stop_times_data = StopsTimes().data
    trips_data = Trips().data
    calendar_data = Calendar().data

    stop_times_data.set_index("stop_id", inplace=True)
    stops_data.set_index("stop_id", inplace=True)
    stops_data_complete = stops_data.join(stop_times_data)

    stops_data_complete.set_index("trip_id", inplace=True)
    trips_data.set_index("trip_id", inplace=True)
    stops_trips_data_complete = stops_data_complete.join(trips_data)


    for line in shapes_data.itertuples():
        line_geom = line.geometry
        line_stops = stops_trips_data_complete.loc[stops_trips_data_complete["shape_id"] == line.shape_id]
        # we need only unique stop from one trip
        line_stops.drop_duplicates(subset="stop_name", inplace=True)
        line_stops.sort_values(by=["stop_sequence"], inplace=True)
        from shapely.ops import split
        from shapely.geometry import LineString

        for stop in line_stops.itertuples():

            projected_point = line_geom.interpolate(line_geom.project(stop.geometry))
            projected_point_buffered = projected_point.buffer(0.00001)
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
            print(new_segment.wkt)
        print("a")


    # #filter by a day
    # service_id_working = calendar_data.loc[calendar_data["monday"] == "1"]["service_id"].to_list()
    # stops_trips_data_complete_filtered_by_a_day = stops_trips_data_complete.loc[stops_trips_data_complete["service_id"].isin(service_id_working)]
    #
    # shapes_data.to_file("lines.geojson", driver='GeoJSON')
    # stops_trips_data_complete_filtered_by_a_day[["geometry"]].to_file("stops.geojson",  driver='GeoJSON')
    print("Hello !")