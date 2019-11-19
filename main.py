from gtfs_builder.gtfs_main.agency_in import Agency
from gtfs_builder.gtfs_main.calendar_in import Calendar
from gtfs_builder.gtfs_main.calendar_dates_in import CalendarDates
from gtfs_builder.gtfs_main.frequencies_in import Frequencies
from gtfs_builder.gtfs_main.routes_in import Routes




if __name__ == '__main__':
    agency_data = Agency().data
    calendar_data = Calendar().data
    calendardates_data = CalendarDates().data
    frequencies_data = Frequencies().data
    routes_data = Routes().data
    print(a)