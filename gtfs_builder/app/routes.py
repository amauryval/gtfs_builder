
from flask import Blueprint
from flask import jsonify
from flask import request

from gtfs_builder.app.core import GtfsMain

# An ugly class to load dataframe on-demand but without mutli user capabilities... only to reduce memory server usage...
# class LoadData:
#     _data = None
#
#     def __init__(self):
#         self._current_data = None
#
#     def load_data(self, area):
#         """
#         unsed
#
#         Exemple:
#
#         >>> data_loader = LoadData()
#         >>> data_loader = LoadData().load_data(area)
#         >>> data_loader.data
#
#         """
#         if self._current_data != area:
#             self._data = io.read_parquet(f"sp_{area}_moving_stops.parq")
#             self._current_data = area
#
#     @property
#     def data(self):
#         return self._data


def gtfs_routes(data, areas_list):

    gtfs_routes = Blueprint(
        f"gtfs",
        __name__,
        template_folder='templates',
        url_prefix=f"/api/v1/gtfs_builder/"
    )

    @gtfs_routes.get("<area>/existing_study_areas")
    def existing_study_areas(area):

        try:

            input_data = jsonify(areas_list)
            input_data.headers.add('Access-Control-Allow-Origin', '*')

            return input_data

        except Exception as exc:
            return jsonify(exception=exc), 204

    @gtfs_routes.get("<area>/moving_nodes_by_date")
    def moving_nodes_by_date(area):
        arg_keys = {
            "current_date": request.args.get("current_date", type=str),
            "bounds": request.args.get("bounds", type=str)
        }
        data_from_area = data[area]
        try:
            bounds = map(float, arg_keys["bounds"].split(","))
            input_data = GtfsMain(data_from_area["data"]).nodes_by_date_from_parquet(arg_keys["current_date"], bounds)

            input_data = jsonify(input_data)
            input_data.headers.add('Access-Control-Allow-Origin', '*')

            return input_data

        except Exception as exc:
            return jsonify(exception=exc), 204

    @gtfs_routes.get("<area>/range_dates")
    def range_dates(area):
        data_from_area = data[area]
        try:
            input_data = GtfsMain(data_from_area["data"]).context_data_from_parquet()

            input_data = jsonify(input_data)
            input_data.headers.add('Access-Control-Allow-Origin', '*')

            return input_data
        except Exception as exc:
            return jsonify(exception=exc), 204

    return gtfs_routes