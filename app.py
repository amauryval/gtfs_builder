import os

from flask import Flask
from flask_cors import CORS

from dotenv import load_dotenv

from gtfs_builder.gtfs_app.routes import gtfs_routes

from geolib import GeoLib

import spatialpandas.io as io


load_dotenv(".gtfs.env")



import re

def str_to_dict_from_regex(str_value: str, regex: str):
    r = re.compile(regex)
    return [m.groupdict() for m in r.finditer(str_value)]

from_mode = os.environ.get("FROM")
if from_mode == "db":
    credentials = {
        **str_to_dict_from_regex(
            os.environ.get("ADMIN_DB_URL"),
            ".+:\/\/(?P<username>.+):(?P<password>.+)@(?P<host>.+):(?P<port>\d{4})\/(?P<database>.+)"
        )[0],
        **{"scoped_session": False}
    }

    session, engine = GeoLib().sqlalchemy_connection(**credentials)
    #to use parallel queries
    # session_factory = sessionmaker(bind=engine)
    # session = scoped_session(session_factory)
elif from_mode == "parquet":
    session = io.read_parquet("stops.parq")



app = Flask(__name__)
CORS(app)
app.register_blueprint(gtfs_routes(session, from_mode))
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
app.config['JSON_SORT_KEYS'] = False

os.environ['ROUTE_RUNNING_STATUS'] = 'start'

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=7000, threaded=False)
