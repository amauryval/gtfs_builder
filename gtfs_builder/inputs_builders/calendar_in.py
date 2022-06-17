from gtfs_builder.core.core import OpenGtfs

import datetime

class Calendar(OpenGtfs):

    def __init__(self, core, input_file: str = "calendar.txt") -> None:

        super(Calendar, self).__init__(core, core.path_data, input_file)

        if self.is_df_empty(self._input_data):
            raise ValueError(f"'{input_file}' is empty")
        self._format_date_column()

    def _format_date_column(self) -> None:
        self._input_data["start_date"] = [datetime.datetime.strptime(row, '%Y%m%d') for row in self._input_data["start_date"]]
        self._input_data["end_date"] = [datetime.datetime.strptime(row, '%Y%m%d') for row in self._input_data["end_date"]]