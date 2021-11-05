

from geolib import GeoLib

from sqlalchemy import and_

from sqlalchemy.sql.expression import literal
from sqlalchemy.sql.expression import literal_column
from sqlalchemy import func

import datetime

def sql_query_to_list(query):
    return [
        {
            column: getattr(row, column)
            for column in row._fields
        }
        for row in query.all()
    ]


class GtfsMain(GeoLib):
    __DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(self, data):
        super().__init__(logger_name=None)

        self._data = data

    def context_data_from_parquet(self):
        return {
            "data_bounds": self._data.geometry.total_bounds,
            "start_date": datetime.datetime.fromtimestamp(min(self._data["start_date"])).strftime(self.__DATE_FORMAT),
            "end_date": datetime.datetime.fromtimestamp(max(self._data["end_date"])).strftime(self.__DATE_FORMAT),
        }

    def nodes_by_date_from_parquet(self, current_date):
        current_date = datetime.datetime.fromisoformat(current_date).timestamp()
        filtered_data = self._data.loc[(self._data["start_date"] <= current_date) & (self._data["end_date"] >= current_date)]
        filtered_data = filtered_data[["stop_code", "x", "y", "route_short_name"]]

        return {
            "data_geojson": filtered_data.to_dict("records")
        }
