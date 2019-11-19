from gtfs_builder.gtfs_core.core import OpenGtfs


class CalendarDates(OpenGtfs):

    def __init__(self, input_file="calendar_dates.txt"):

        super(CalendarDates, self).__init__(input_file)

