from typing import List
from typing import Optional

from gtfs_builder.core.core import OpenGtfs


class Routes(OpenGtfs):

    _ROUTE_TYPE_MAPPING = {
        "0": "tramway",
        "1": "metro",
        "2": "train",
        "3": "bus",
        "4": "ferry",
        "5": "cable_tramway",
        "6": "cableway",
        "7": "funicular"
    }

    def __init__(self, geo_tools_core, transport_modes: Optional[List[str]] = None, input_file="routes.txt"):

        super(Routes, self).__init__(geo_tools_core, geo_tools_core.path_data, input_file)

        self._input_file = input_file

        self.__remap_route_type()
        if transport_modes is not None:
            self.__filter_by_route_types(transport_modes)

        if self.is_df_empty(self._input_data):
            raise ValueError(f"'{input_file}' is empty")

    def __remap_route_type(self):
        self._input_data.replace(
            {"route_type": self._ROUTE_TYPE_MAPPING},
            inplace=True
        )

    def __filter_by_route_types(self, transport_modes: List[str]):
        transport_mode_column = "route_type"
        self._input_data = self._input_data.loc[self._input_data[transport_mode_column].isin(transport_modes)]

        if self.is_df_empty(self._input_data):
            raise ValueError(f"'{self._input_file}' is empty: check transport mode value(s) ({transport_mode_column}")