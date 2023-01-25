import os
from typing import Dict

from spatialpandas import io
from spatialpandas import GeoDataFrame

from gtfs_builder.app.config import settings


def _get_data(area: str) -> GeoDataFrame:
    return io.read_parquet(
        os.path.join(os.getcwd(), settings.DATA_DIR, f"{area.lower()}_moving_stops.parq"),
        columns=["start_date", "end_date", "x", "y", "geometry", "route_long_name", "route_type"]).astype({
            "start_date": "uint32",
            "end_date": "uint32",
            "geometry": "Point[float64]",
            "x": "float32",
            "y": "float32",
            "route_type": "uint8",
            "route_long_name": "string",
        }
    )


def input_data() -> Dict:
    return {
        area: _get_data(area)
        for area in settings.AREAS
    }

def to_json():
    import pandas as pd
    for title, data in input_data().items():
        print(title)
        data = data[[
            "start_date",
            "end_date",
            "x", "y",
            "route_long_name",
            "route_type"
        ]]
        data.reset_index(inplace=True)
        #data = data.drop(['index'], axis=1)
        routes_indexed = pd.DataFrame(data["route_long_name"].unique()).reset_index()
        routes_indexed.columns = ['route_id', 'route_long_name']
        data = data.merge(routes_indexed, how="left", on="route_long_name")

        data.to_json(f"data/{title}_gtfsData.json", orient="records")
        routes_indexed.to_json(f"data/{title}_routeGtfsData.json", orient="records")


to_json()