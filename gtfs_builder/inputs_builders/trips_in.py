from gtfs_builder.core.core import OpenGtfs


class Trips(OpenGtfs):

    _DIRECTION_FIELD_NAME = "direction_id"
    _DIRECTON_MAPPING = {
        "0": "back",
        "1": "forth"
    }

    def __init__(self, core, input_file: str = "trips.txt") -> None:

        super(Trips, self).__init__(core, core.path_data, input_file)
        self.__clean_direction_id()

        if self.is_df_empty(self._input_data):
            raise ValueError(f"'{input_file}' is empty")

    def __clean_direction_id(self) -> None:
        if self._DIRECTION_FIELD_NAME not in self._input_data.columns.to_list():
            self._input_data.loc[:, self._DIRECTION_FIELD_NAME] = 3

        # None value are set to 3
        self._input_data[self._DIRECTION_FIELD_NAME] = [
            "3" if row not in self._DIRECTON_MAPPING.keys() else row
            for row in self._input_data[self._DIRECTION_FIELD_NAME]
        ]
