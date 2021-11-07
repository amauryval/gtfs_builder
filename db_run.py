import os

from gtfs_builder.main import GtfsFormater
from dotenv import load_dotenv

import json





if __name__ == '__main__':

    with open("params.json") as input_file:
        data = json.loads(input_file.read())
    data_filtered = data["ter"]

    GtfsFormater(
        study_area_name=data_filtered["study_area_name"],
        data_path=data_filtered["input_data_dir"],
        transport_modes=data_filtered["transport_modes"],
        date_mode=data_filtered["date_mode"],
        date=data_filtered["date"],
        build_shape_data=data_filtered["build_shape_id"],
        interpolation_threshold=data_filtered["interpolation_threshold"]
    )