import pandas as pd

import numpy as np

import dateutil


class DfOptimizer:
    
    def __init__(self, df, category_count_limit=50):
        self._df = df
        self._category_count_limit = category_count_limit

        self._source_df_mem = self._get_memory_usage(self._df)
        self._optimize()
        self._result_df_mem = self._get_memory_usage(self._df)

    def _optimize(self):
        for col in self._df.columns:

            if not self._df[col].isnull().all():

                if not isinstance(self._df[col].dtype, object):

                    is_integer_column = self._check_if_column_contains_integers(col)
                    if is_integer_column:
                        self._format_integer_column(col)
                    else:
                        self._format_float_column(col)

                elif self._df[col].dtype == object:
                    if not self._convert_to_datetime(col):
                        if isinstance(self._df[col].iat[0], str) and self._df[col].nunique() <= 50:
                            self._df[col] = self._df[col].astype('category')

    def _convert_to_datetime(self, col):
        try:
            self._df[col] = self._df[col].apply(lambda x: dateutil.parser.parse(x, fuzzy=True))
            return True
        except (ValueError, OverflowError, TypeError):
            return False

    def _check_if_column_contains_integers(self, col):
        # Integer does not support NA, therefore, NA needs to be filled
        if not np.isfinite(self._df[col]).all():
            min = self._df[col].min()
            self._df[col].fillna(min - 1, inplace=True)

        # tests if column can be converted to an integer
        as_int = self._df[col].fillna(0).astype(np.int64)
        result = (self._df[col] - as_int)
        result = result.sum()
        if result > -0.01 and result < 0.01:
            return True

    def _format_integer_column(self, col):

        max = self._df[col].max()
        min = self._df[col].min()

        if min >= 0:
            if max < 255:
                self._df[col] = self._df[col].astype(np.uint8)
            elif max < 65535:
                self._df[col] = self._df[col].astype(np.uint16)
            elif max < 4294967295:
                self._df[col] = self._df[col].astype(np.uint32)
            else:
                self._df[col] = self._df[col].astype(np.uint64)

        else:
            if min > np.iinfo(np.int8).min and max < np.iinfo(np.int8).max:
                self._df[col] = self._df[col].astype(np.int8)
            elif min > np.iinfo(np.int16).min and max < np.iinfo(np.int16).max:
                self._df[col] = self._df[col].astype(np.int16)
            elif min > np.iinfo(np.int32).min and max < np.iinfo(np.int32).max:
                self._df[col] = self._df[col].astype(np.int32)
            elif min > np.iinfo(np.int64).min and max < np.iinfo(np.int64).max:
                self._df[col] = self._df[col].astype(np.int64)

    def _format_float_column(self, col):
        self._df[col].astype(np.float32)

    def _get_memory_usage(self, df):
        if isinstance(df, pd.DataFrame):
            mem_usage = df.memory_usage(deep=True).sum()
        else:
            mem_usage = df.memory_usage(deep=True)

        mem_usage = mem_usage / 1024 ** 2
        return f"{mem_usage:03.2f} MB"

    @property
    def data(self):
        return self._df

    @property
    def memory_usage(self):
        return f"{self._source_df_mem } => {self._result_df_mem}"
