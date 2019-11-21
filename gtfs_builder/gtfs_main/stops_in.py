from gtfs_builder.gtfs_core.core import OpenGtfs


class Stops(OpenGtfs):

    def __init__(self, input_file="stops.txt"):

        super(Stops, self).__init__(input_file)

        self.__get_real_stops()
        self.__build_stop_points()

    def __get_real_stops(self):
        self._input_data = self._input_data.loc[self._input_data["location_type"] == "0"]

    def __build_stop_points(self):
        self._input_data = self.gdf_from_df_long_lat(
            self._input_data,
            "stop_lon",
            "stop_lat"
        )