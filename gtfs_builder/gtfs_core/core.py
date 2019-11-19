import os

import pandas as pd

import json

from core.data_helper import DfOptimizer

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

    _DEFAULT_JSON_ATTRS = json.loads(
        open(os.path.join(os.getcwd(), "inputs_attrs.json")).read()
    )
    _DEFAULT_INPUT_DATA_PATH = os.path.join(
        os.getcwd(),
        "input_data"
    )
    _SEPARATOR = ","

    def __init__(self, input_file):
        """

        :param input_file: the name of the input file with its extension
        :type input_file: str
        """
        default_field_and_type = self._DEFAULT_JSON_ATTRS[input_file]

        print(f"Opening {input_file}")

        self._input = input_file
        self._check_input_path()
        self._open_input_data(default_field_and_type)

    def _check_input_path(self):
        """

        :return: None
        """
        if os.path.isfile(self._input):
            self.file_path = self._input
            return

        default_path = os.path.join(
                self._DEFAULT_INPUT_DATA_PATH,
                self._input,
        )
        if os.path.isfile(default_path):
            self.file_path = default_path
            return

        raise InputDataNotFound("Input data path not found!")

    def _open_input_data(self, default_fields_and_type):
        """

        :type default_fields_and_type: dict
        :return: pandas.DataFrame
        """

        if len(default_fields_and_type) == 0:
            print("Default fields not defined")
        try:

            # because usecols on read_csv sucks!
            input_data_columns = pd.read_csv(
                self.file_path,
                sep=self._SEPARATOR,
                nrows=0,
            ).columns
            # filter on default_fields_and_type
            columns_not_found = set(default_fields_and_type.keys()) - set(input_data_columns)
            print(f"[{', '.join(columns_not_found)}] not found on input data: {self._input}")
            for column_bot_found in columns_not_found:
                del default_fields_and_type[column_bot_found]

            # ok go to open input data
            input_data = pd.read_csv(
                self.file_path,
                sep=self._SEPARATOR,
                dtype=default_fields_and_type,
                usecols=default_fields_and_type.keys(),
            )

            input_data = DfOptimizer(input_data)
            self._input_data = input_data.data
            print(input_data.memory_usage)

        except ValueError as err:
            raise OpeningProblem(err)

    @property
    def data(self):
        """
        Return dataframe

        :return: dataframe
        :rtype: pandas.DataFrame
        """
        if not self._is_df_empty(self._input_data):
            return self._input_data

    def _is_df_empty(self, df):
        """
        Check if dataframe is empty

        :param df: input dataframe
        :type df: padnas.DataFrame
        :return: return True if dataframe is empty
        :rtype: boolean
        """

        if df.shape[0] == 0:
            print("Dataframe is empty")
            return True
