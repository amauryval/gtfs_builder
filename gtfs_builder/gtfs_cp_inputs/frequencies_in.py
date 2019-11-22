from gtfs_builder.gtfs_core.core import OpenGtfs


class Frequencies(OpenGtfs):

    def __init__(self, input_file="frequencies.txt"):

        super(Frequencies, self).__init__(input_file)

