import os

from gtfs_builder.main import GtfsFormater
from gtfs_builder.push_db import PushDb

import json

from dotenv import load_dotenv

load_dotenv(".gtfs_builder.env")


if __name__ == '__main__':

    data_builder = False
    data_push = True

    area_names = ["ter"]

    for area_name in area_names:
        with open("params.json") as input_file:
            data = json.loads(input_file.read())
        data_filtered = data[area_name]



        if data_builder:
            GtfsFormater(
                study_area_name=data_filtered["study_area_name"],
                data_path=data_filtered["input_data_dir"],
                transport_modes=data_filtered["transport_modes"],
                date_mode=data_filtered["date_mode"],
                date=data_filtered["date"],
                build_shape_data=data_filtered["build_shape_id"],
                interpolation_threshold=data_filtered["interpolation_threshold"],
                multiprocess=data_filtered["multiprocess"],
            )


    if data_push:
        PushDb(area_names).run()