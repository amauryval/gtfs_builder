from gtfs_builder.gtfs_core.core import OpenGtfs


class Trips(OpenGtfs):

    def __init__(self, input_file="trips.txt"):

        super(Trips, self).__init__(input_file)
