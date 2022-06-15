from gtfs_builder.core.core import OpenGtfs


class StopsTimes(OpenGtfs):

    def __init__(self, core, input_file="stop_times.txt"):

        super(StopsTimes, self).__init__(core, core.path_data, input_file)

        if self.is_df_empty(self._input_data):
            raise ValueError(f"'{input_file}' is empty")
