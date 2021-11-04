import os

from gtfs_builder.main import GtfsFormater
from dotenv import load_dotenv

load_dotenv(".gtfs.env")



if __name__ == '__main__':

    GtfsFormater(
        study_area_name=os.environ["STUDY_AREA_NAME"],
        data_path=os.environ["INPUT_DATA_DIR"],
        transport_modes=os.environ["TRANSPORT_MODES"].split(","),
        days=os.environ["DAYS"].split(","),
        build_shape_data=bool(int(os.environ["BUILD_SHAPE_ID"]))
    )