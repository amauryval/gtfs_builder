from gtfs_builder.gtfs_core.core import OpenGtfs


class Trips(OpenGtfs):
    _DIRECTON_MAPPING = {
        "0": "back",
        "1": "forth"
    }

    def __init__(self, geo_tools_core, input_file="trips.txt"):

        super(Trips, self).__init__(geo_tools_core, input_file)
        self.__remap_direction_id()

    def __remap_direction_id(self):
        self._input_data.replace(
            {"direction_id": self._DIRECTON_MAPPING},
            inplace=True
        )
