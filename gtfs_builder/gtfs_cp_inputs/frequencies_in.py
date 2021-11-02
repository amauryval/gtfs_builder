from gtfs_builder.gtfs_core.core import OpenGtfs


class Frequencies(OpenGtfs):

    def __init__(self, geo_tools_core, input_file="frequencies.txt"):

        super(Frequencies, self).__init__(geo_tools_core, geo_tools_core.path_data, input_file)

        if self.is_df_empty(self._input_data):
            raise ValueError(f"'{input_file}' is empty")
