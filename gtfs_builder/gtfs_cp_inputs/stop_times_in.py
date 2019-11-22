from gtfs_builder.gtfs_core.core import OpenGtfs


class StopsTimes(OpenGtfs):

    def __init__(self, input_file="stop_times.txt"):

        super(StopsTimes, self).__init__(input_file)
