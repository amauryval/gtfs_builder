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

    def __init__(self, geo_tools_core, input_file="routes.txt"):

        super(Routes, self).__init__(geo_tools_core, input_file)
        self.__remap_route_type()

    def __remap_route_type(self):
        self._input_data.replace(
            {"route_type": self._ROUTE_TYPE_MAPPING},
            inplace=True
        )

