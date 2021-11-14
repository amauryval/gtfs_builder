
import datetime

def sql_query_to_list(query):
    return [
        {
            column: getattr(row, column)
            for column in row._fields
        }
        for row in query.all()
    ]


class GtfsMain:
    __DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(self, data):
        self._data = data

    def context_data_from_parquet(self):
        return {
            "data_bounds": self._data.geometry.total_bounds,
            "start_date": datetime.datetime.fromtimestamp(min(self._data["start_date"])).strftime(self.__DATE_FORMAT),
            "end_date": datetime.datetime.fromtimestamp(max(self._data["end_date"])).strftime(self.__DATE_FORMAT),
        }

    def nodes_by_date_from_parquet(self, current_date, bounds):

        current_date = datetime.datetime.fromisoformat(current_date).timestamp()

        filtered_data = self._data.loc[(self._data["start_date"] <= current_date) & (self._data["end_date"] >= current_date)]
        bounds = list(bounds)
        filtered_data = filtered_data.cx[bounds[0]:bounds[2], bounds[1]:bounds[3]]
        filtered_data = filtered_data[["x", "y", "route_long_name", "route_type"]]

        return {
            "data_geojson": filtered_data.to_dict("records")
        }
