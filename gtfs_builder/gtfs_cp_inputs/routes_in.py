from typing import List
from typing import Optional

from gtfs_builder.gtfs_core.core import OpenGtfs


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

        super(Routes, self).__init__(geo_tools_core, input_file)
        self.__remap_route_type()
        if transport_modes is not None:
            self.__filter_by_route_types(transport_modes)

    def __remap_route_type(self):
        self._input_data.replace(
            {"route_type": self._ROUTE_TYPE_MAPPING},
            inplace=True
        )

    def __filter_by_route_types(self, transport_modes: List[str]):
        self._input_data = self._input_data.loc[self._input_data["route_type"].isin(transport_modes)]


