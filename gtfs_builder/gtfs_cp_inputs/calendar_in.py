from gtfs_builder.gtfs_core.core import OpenGtfs


class Calendar(OpenGtfs):

    def __init__(self, geo_tools_core, input_file="calendar.txt"):

        super(Calendar, self).__init__(geo_tools_core, input_file)
