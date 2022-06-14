import os

from dotenv import load_dotenv
from geolib import GeoLib
from geolib.misc.extraction import str_to_dict_from_regex

from gtfs_builder.db import Base
from gtfs_builder.main import GtfsFormater

import json

load_dotenv()


def get_db_session():
    credentials = {
        **str_to_dict_from_regex(
            os.environ.get("ADMIN_DB_URL"),
            ".+:\/\/(?P<username>.+):(?P<password>.+)@(?P<host>.+):(?P<port>\d{4})\/(?P<database>.+)"
        ),
        **{"scoped_session": True}
    }

    session, _ = GeoLib().sqlalchemy_connection(**credentials)
    return session

def build_data(input_params_file_path: str) -> None:
    # overwrite db
    db_session = get_db_session()
    Base.metadata.drop_all(db_session.bind)

    with open(input_params_file_path, encoding="UTF-8") as input_file:
        input_params = json.loads(input_file.read())

    for input_data in input_params:
        GtfsFormater(
            study_area_name=input_data["study_area_name"],
            data_path=input_data["input_data_dir"],
            transport_modes=input_data["transport_modes"],
            date_mode=input_data["date_mode"],
            date=input_data["date"],
            build_shape_data=input_data["build_shape_id"],
            interpolation_threshold=input_data["interpolation_threshold"],
            multiprocess=input_data["multiprocess"],
            output_format="db",
            db_mode="append"
        )


if __name__ == '__main__':
    build_data("params.json")


