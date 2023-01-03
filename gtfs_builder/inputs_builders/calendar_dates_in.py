from gtfs_builder.core.core import OpenGtfs

import datetime


class CalendarDates(OpenGtfs):

    def __init__(self, core, input_file: str = "calendar_dates.txt") -> None:

        super(CalendarDates, self).__init__(core, core.path_data, input_file)

        if self.is_df_empty(self._input_data):
            raise ValueError(f"'{input_file}' is empty")

        self._format_date_column()

    def _format_date_column(self) -> None:
        self._input_data["date"] = [datetime.datetime.strptime(row, '%Y%m%d') for row in self._input_data["date"]]
