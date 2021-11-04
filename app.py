import os

from flask import Flask
from flask_cors import CORS

from dotenv import load_dotenv

from gtfs_builder.app.routes import gtfs_routes

from spatialpandas import io


load_dotenv(".gtfs.env")

session = io.read_parquet(f"{os.environ['study_area_name']}_moving_stops.parq")
# session2 = io.read_parquet(f"{os.environ['study_area_name']}2_moving_stops.parq")


app = Flask(__name__)
CORS(app)
app.register_blueprint(gtfs_routes(session))
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
app.config['JSON_SORT_KEYS'] = False


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=7000, threaded=False)
