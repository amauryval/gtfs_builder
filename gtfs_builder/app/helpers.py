import os
from typing import Dict

from spatialpandas import io
from spatialpandas import GeoDataFrame

from gtfs_builder.app.config import settings


def _get_data(area: str) -> GeoDataFrame:
    return GeoDataFrame(io.read_parquet(
        os.path.join(os.getcwd(), settings.DATA_DIR, f"{area.lower()}_moving_stops.parq"),
        columns=["start_date", "end_date", "x", "y", "geometry", "route_long_name", "route_type"]).astype({
        "start_date": "uint32",
        "end_date": "uint32",
        "geometry": "Point[float64]",
        "x": "category",
        "y": "category",
        "route_type": "category",
        "route_long_name": "category",
    })
    )


def input_data() -> Dict:
    return {
        area: _get_data(area)
        for area in settings.AREAS
    }
