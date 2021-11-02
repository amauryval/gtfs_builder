from gtfs_builder.gtfs_core.core import OpenGtfs


class Trips(OpenGtfs):
    _DIRECTON_MAPPING = {
        "0": "back",
        "1": "forth"
    }

    def __init__(self, geo_tools_core, input_file="trips.txt"):

        super(Trips, self).__init__(geo_tools_core, geo_tools_core.path_data, input_file)
        self.__remap_direction_id()

        if self.is_df_empty(self._input_data):
            raise ValueError(f"'{input_file}' is empty")

    def __remap_direction_id(self):
        self._input_data.replace(
            {"direction_id": self._DIRECTON_MAPPING},
            inplace=True
        )
