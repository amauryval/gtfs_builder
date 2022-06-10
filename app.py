import os
from flask import Flask

from gtfs_builder.app.routes import gtfs_routes

from spatialpandas import io
from spatialpandas import GeoDataFrame

from dotenv import load_dotenv

load_dotenv(".gtfs_builder.env")


def get_data(study_area):
    return GeoDataFrame(io.read_parquet(
            f"{study_area}_moving_stops.parq",
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

data = {
    study_area: {
        "data": get_data(study_area),
        "study_area": study_area
    }
    for study_area in os.environ["AREAS"].split(",")
}

app = Flask(__name__)
app.register_blueprint(gtfs_routes(data, os.environ["AREAS"].split(",")))

app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
app.config['JSON_SORT_KEYS'] = False


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=7000, threaded=False)
