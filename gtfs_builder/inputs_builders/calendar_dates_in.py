from gtfs_builder.core.core import OpenGtfs

import pandas as pd


class CalendarDates(OpenGtfs):

    def __init__(self, geo_tools_core, input_file="calendar_dates.txt"):

        super(CalendarDates, self).__init__(geo_tools_core, geo_tools_core.path_data, input_file)

        if self.is_df_empty(self._input_data):
            raise ValueError(f"'{input_file}' is empty")

        # self._format_date_column()

    def _format_date_column(self):
        self._input_data["date"] = pd.to_datetime(self._input_data['date'], format='%Y%m%d')