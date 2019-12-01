from gtfs_builder.gtfs_core.core import OpenGtfs


class Agency(OpenGtfs):

    def __init__(self, geo_tools_core, input_file="agency.txt"):

        super(Agency, self).__init__(geo_tools_core, input_file)

