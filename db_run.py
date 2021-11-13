from gtfs_builder.main import GtfsFormater

import json

if __name__ == '__main__':

    with open("params.json") as input_file:
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
        )
