
import datetime
from typing import Dict, List, Union

import geopandas as gpd

from gtfs_builder.app.global_values import date_format


class GtfsMain:

    def __init__(self, data: gpd.GeoDataFrame):
        self._data = data

    def context_data_from_parquet(self) -> Dict:
        return {
            "data_bounds": self._data.geometry.total_bounds,
            "start_date": datetime.datetime.fromtimestamp(min(self._data["start_date"])).strftime(date_format),
            "end_date": datetime.datetime.fromtimestamp(max(self._data["end_date"])).strftime(date_format),
        }

    def nodes_by_date_from_parquet(self, current_date: str, bounds: Union[List[str], List[float]]) -> Dict:

        current_date = datetime.datetime.fromisoformat(current_date).timestamp()

        filtered_data = self._data.loc[(self._data["start_date"] <= current_date) & (self._data["end_date"] >= current_date)]
        bounds = list(bounds)
        filtered_data = filtered_data.cx[bounds[0]:bounds[2], bounds[1]:bounds[3]]
        filtered_data = filtered_data[["x", "y", "route_long_name", "route_type"]]

        # TODO remove data_geojson key
        return filtered_data.to_dict("records")
