from gtfs_builder.core.core import OpenGtfs

import numpy as np


class Stops(OpenGtfs):

    def __init__(self, core, input_file: str = "stops.txt", use_original_epsg: bool = False) -> None:

        super(Stops, self).__init__(core, core.path_data, input_file, use_original_epsg)
        self._core = core

        self.__check_if_stops_found()
        self.__get_real_stops()
        self.__build_stop_points()

        self._input_data = self._reproject_gdf(
            self._input_data
        )

        if self.is_df_empty(self._input_data):
            raise ValueError(f"'{input_file}' is empty")

    def __check_if_stops_found(self) -> None:
        if "stop_code" not in self._input_data.columns:
            self._core.logger.info('Creating a "stop_code" column...')
            self._input_data["stop_code"] = [f"{stop_id}_{enum}" for enum, stop_id in enumerate(self._input_data["stop_id"])]

    def __get_real_stops(self) -> None:
        location_type_name = "location_type"
        if location_type_name in self._input_data.columns:
            self._input_data = self._input_data.loc[self._input_data["location_type"] == "0"]
        else:
            self._core.logger.warning(f"'{location_type_name}' field not found")

    def __build_stop_points(self) -> None:
        self._input_data = self.gdf_from_df_long_lat(
            self._input_data,
            "stop_lon",
            "stop_lat"
        )
