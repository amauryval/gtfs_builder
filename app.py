import os
from flask import Flask
from geospatial_lib import GeoSpatialLib

from gtfs_builder.app.routes_from_db import gtfs_routes_from_db
from gtfs_builder.app.routes_from_files import gtfs_routes_from_files

from spatialpandas import io
from spatialpandas import GeoDataFrame

from dotenv import load_dotenv

from gtfs_builder.main import str_to_dict_from_regex

load_dotenv()


def get_db_session():
    credentials = {
        **str_to_dict_from_regex(
            os.environ.get("ADMIN_DB_URL"),
            r".+:\/\/(?P<username>.+):(?P<password>.+)@(?P<host>.+):(?P<port>\d{4})\/(?P<database>.+)"
        ),
        **{"scoped_session": True}
    }

    session, _ = GeoSpatialLib().sqlalchemy_connection(**credentials)
    return session


def get_data(study_area):
    return GeoDataFrame(io.read_parquet(
            os.path.join(os.getcwd(), "data", f"{study_area.lower()}_moving_stops.parq"),
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


app = Flask(__name__)
mode = os.environ["MODE"]
if mode == 'file':
    data = {
        study_area: {
            "data": get_data(study_area),
            "study_area": study_area
        }
        for study_area in os.environ["AREAS"].split(",")
    }

    app.register_blueprint(gtfs_routes_from_files(data, os.environ["AREAS"].split(",")))
elif mode == "db":
    app.register_blueprint(gtfs_routes_from_db(get_db_session()))

app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
app.config['JSON_SORT_KEYS'] = False


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=7000, threaded=False)
