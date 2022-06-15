import os
from typing import Dict

import pandas as pd
import geopandas as gpd

from shapely.geometry import LineString

import json

class InputDataNotFound(Exception):
    pass


class OpeningProblem(Exception):
    pass


class OpenGtfs:
    """
    Pytnon class to open and optimize a GTFS file. It returns a dataframe

    Methods:
        * data: return the dataframe from the self._input_data variable

    """

    _SEPARATOR = ","
    _OUTPUT_ESPG_KEY = "output_epsg"

    _COMPUTED_FILE_TAGS = ["_computed", "_updated"]

    _DEFAULT_EPSG = 4326

    def __init__(self, core, path_data: str, input_file: str, use_original_epsg: bool = False) -> None:
        """

        :param input_file: the name of the input file with its extension
        :type input_file: str
        """
        self.core = core
        self._path_data = path_data

        with open(os.path.join(self._path_data, "inputs_attrs.json")) as output:
            config_file_path = json.loads(output.read())

        input_file_base = str(input_file)
        for tag in self._COMPUTED_FILE_TAGS:
            input_file_base = input_file_base.replace(tag, "")
        default_field_and_type = config_file_path[input_file_base]

        self.core.logger.info(f"Opening {input_file}")

        self._input = input_file

        if not use_original_epsg:
            self._to_epsg = config_file_path[self._OUTPUT_ESPG_KEY]
        else:
            self._to_epsg = None

        self._check_input_path()
        self._open_input_data(default_field_and_type)

    def _check_input_path(self) -> None:
        """

        :return: None
        """
        if os.path.isfile(self._input):
            self.file_path = self._input
            return

        default_path = os.path.join(
                self._path_data,
                self._input,
        )

        if os.path.isfile(default_path):
            self.file_path = default_path
            return

        raise InputDataNotFound("Input data path not found!")

    def _open_input_data(self, default_fields_and_type: Dict) -> None:
        """

        :type default_fields_and_type: dict
        :return: pandas.DataFrame
        """

        if len(default_fields_and_type) == 0:
            self.core.logger.warning("Default fields not defined")
        try:
            # because usecols on read_csv sucks!
            input_data_columns = pd.read_csv(
                self.file_path,
                sep=self._SEPARATOR,
                nrows=0,
            ).columns
            # filter on default_fields_and_type
            columns_not_found = set(default_fields_and_type.keys()) - set(input_data_columns)

            if len(columns_not_found) > 0:
                self.core.logger.warning(f"[{', '.join(columns_not_found)}] not found on input data: {self._input}")
                for column_bot_found in columns_not_found:
                    del default_fields_and_type[column_bot_found]

            # ok go to open input data
            self._input_data = pd.read_csv(
                self.file_path,
                sep=self._SEPARATOR,
                dtype=default_fields_and_type,
                usecols=default_fields_and_type.keys(),
            )

        except ValueError as err:
            raise OpeningProblem(err)

    @property
    def data(self) -> pd.DataFrame:
        """
        Return dataframe

        :return: dataframe
        :rtype: pandas.DataFrame
        """
        if not self.is_df_empty(self._input_data):
             return self._input_data

    def gdf_from_df_long_lat(self, df: pd.DataFrame, longitude: str, latitude: str) -> gpd.GeoDataFrame:
        """
        Create a geodataframe with longitude and latitude fields

        :param df: dataframe
        :type df: pandas.DataFrame
        :param longitude: longitude field name
        :type longitude: string
        :param latitude: latitude field name
        :type latitude: string
        :return: geodataframe with point
        :rtype: geopandas.GeodataFrame with point
        """

        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df[longitude], df[latitude]),
            # crs={'init': f"epsg:{self._DEFAULT_EPSG}"}
        )

        gdf.drop([longitude, latitude], axis=1, inplace=True)

        return gdf

    def group_by_id_from_point_to_create_linestring(self, gdf: gpd.GeoDataFrame, id_field: str, sequence_field: str, geom_field: str = "geometry") -> gpd.GeoDataFrame:
        """

        :param gdf: geodataframe
        :type gdf: geopandas.GeodataFrame
        :param geom_field: geometry field name
        :type geom_field: string
        :return: geodataframe with linestrings for each id
        :rtype: geopandas.GeodataFrame with point
        """
        gdf.sort_values(by=[id_field, sequence_field], inplace=True)
        gdf = gdf.groupby(id_field)[geom_field].apply(lambda x: LineString(x.tolist())).reset_index(name="geometry")
        gdf = gpd.GeoDataFrame(
            gdf,
            geometry=gdf["geometry"],
            # crs={'init': f"epsg:{self._DEFAULT_EPSG}"}
        )
        return gdf

    def is_df_empty(self, df: pd.DataFrame) -> bool:
        """
        Check if dataframe is empty

        :param df: input dataframe
        :type df: padnas.DataFrame
        :return: return True if dataframe is empty
        :rtype: boolean
        """

        if df.shape[0] == 0:
            return True
        return False

    def _reproject_gdf(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        # if self._to_epsg is not None:
        #     gdf = gdf.to_crs(epsg=self._to_epsg)
        return gdf