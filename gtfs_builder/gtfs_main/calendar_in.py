from gtfs_builder.gtfs_core.core import OpenGtfs


class Calendar(OpenGtfs):

    def __init__(self, input_file="calendar.txt"):

        super(Calendar, self).__init__(input_file)
