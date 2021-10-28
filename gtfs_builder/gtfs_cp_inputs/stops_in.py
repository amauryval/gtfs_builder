from gtfs_builder.gtfs_core.core import OpenGtfs

from uuid import uuid4

class Stops(OpenGtfs):

    def __init__(self, geo_tools_core, input_file="stops.txt", use_original_epsg=False):

        super(Stops, self).__init__(geo_tools_core, input_file, use_original_epsg)
        self._core = geo_tools_core

        self.__check_if_stops_found()
        self.__get_real_stops()
        self.__build_stop_points()

        self._input_data = self._reproject_gdf(
            self._input_data
        )

    def __check_if_stops_found(self):
        if "stop_code" not in self._input_data.columns:
            self._core.logger.info('Creating a "stop_code" column...')
            self._input_data.loc[:, "stop_code"] = str(uuid4())

    def __get_real_stops(self):
        self._input_data = self._input_data.loc[self._input_data["location_type"] == "0"]

    def __build_stop_points(self):
        self._input_data = self.gdf_from_df_long_lat(
            self._input_data,
            "stop_lon",
            "stop_lat"
        )
