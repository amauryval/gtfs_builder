from core.core import OpenGtfs

class Agency(OpenGtfs):

    def __init__(self, input_file="agency.txt"):

        super(Agency, self).__init__(input_file)

