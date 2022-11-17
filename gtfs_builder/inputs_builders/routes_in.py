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

    __COLUMNS_TO_ADD = ["route_text_color"]

    def __init__(self, core, transport_modes: Optional[List[str]] = None, input_file: str = "routes.txt") -> None:

        super(Routes, self).__init__(core, core.path_data, input_file)

        self._input_file = input_file

        if transport_modes is not None:
            self.__filter_by_route_types(transport_modes)

        self.__check_columns()

        if self.is_df_empty(self._input_data):
            raise ValueError(f"'{input_file}' is empty")

    def __get_available_transport_modes(self) -> None:
        transport_found = list(self._input_data[self.__TRANSPORT_MODE_COLUMN].unique())
        self._available_transport_modes = [
            value
            for transport_key, value in self._ROUTE_TYPE_MAPPING.items()
            if transport_key in transport_found
        ]

    def __filter_by_route_types(self, transport_modes: List[str]) -> None:
        self.__get_available_transport_modes()
        transport_modes_id_to_use = [
            transport_key
            for transport_key, value in self._ROUTE_TYPE_MAPPING.items()
            if value in transport_modes
        ]
        self._input_data = self._input_data.loc[
            self._input_data[self.__TRANSPORT_MODE_COLUMN].isin(transport_modes_id_to_use)
        ]
        if self.is_df_empty(self._input_data):
            raise ValueError(f"'{self._input_file}' is empty: check transport mode value(s) "
                             f"({self.__TRANSPORT_MODE_COLUMN}: Use one of : "
                             f"{', '.join(self._available_transport_modes)}")

    def __check_columns(self) -> None:
        existing_columns = self._input_data.columns.to_list()
        for col in self.__COLUMNS_TO_ADD:
            if col not in existing_columns:
                self._input_data.loc[:, col] = ""
