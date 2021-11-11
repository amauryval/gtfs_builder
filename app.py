from flask import Flask
from flask_cors import CORS

from gtfs_builder.app.routes import gtfs_routes

import geopandas as gpd

from dotenv import load_dotenv

load_dotenv(".gtfs_builder.env")


areas_list = ["ter", "toulouse"]


data = {
    study_area: {
        "data": gpd.read_parquet(f"{study_area}_moving_stops.parq"),
        "study_area": study_area
    }
    for study_area in areas_list
}


app = Flask(__name__)
CORS(app)
app.register_blueprint(gtfs_routes(data, areas_list))

app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
app.config['JSON_SORT_KEYS'] = False


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=7000, threaded=False)
