from typing import List
from typing import Optional

from gtfs_builder.core.core import OpenGtfs


class Routes(OpenGtfs):
    __TRANSPORT_MODE_COLUMN = "route_type"

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

        if transport_modes is not None:
            self.__filter_by_route_types(transport_modes)

        if self.is_df_empty(self._input_data):
            raise ValueError(f"'{input_file}' is empty")

    def __get_available_transport_modes(self):
        transport_found = list(self._input_data[self.__TRANSPORT_MODE_COLUMN].unique())
        self._available_transport_modes = dict(filter(lambda x: x[0] in transport_found, self._ROUTE_TYPE_MAPPING.items())).values()


    def __filter_by_route_types(self, transport_modes: List[str]):
        self.__get_available_transport_modes()

        transport_modes_to_use = dict(filter(lambda x: x[-1] in transport_modes, self._ROUTE_TYPE_MAPPING.items()))
        self._input_data = self._input_data.loc[self._input_data[self.__TRANSPORT_MODE_COLUMN].isin(transport_modes_to_use.keys())]
        if self.is_df_empty(self._input_data):
            raise ValueError(f"'{self._input_file}' is empty: check transport mode value(s) ({self.__TRANSPORT_MODE_COLUMN}: Use one of : {', '.join(self._available_transport_modes)}")