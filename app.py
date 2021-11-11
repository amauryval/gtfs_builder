from flask import Flask
from flask_cors import CORS

from gtfs_builder.app.routes import gtfs_routes

from spatialpandas import io

areas_list = ["ter", "toulouse"]

sessions = [
    {
        "data": io.read_parquet(f"{study_area}_moving_stops.parq"),
        "study_area": study_area
    }
    for study_area in areas_list
]


app = Flask(__name__)
CORS(app)

for session in sessions:
    app.register_blueprint(gtfs_routes(session["data"], session["study_area"], areas_list))

app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
app.config['JSON_SORT_KEYS'] = False


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=7000, threaded=False)
