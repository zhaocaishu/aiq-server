import abc

import pandas as pd
import numpy as np


def mad_filter(x: pd.DataFrame):
    """Robust statistics for outlier filter:
        mean(x) = median(x)
        std(x) = MAD(x) * 1.4826
    Reference:
        https://en.wikipedia.org/wiki/Median_absolute_deviation.
    """
    x = x - x.median()
    mad = x.abs().median()
    x = np.clip(x / mad / 1.4826, -3, 3)
    return x


class Processor(abc.ABC):
    def fit(self, df: pd.DataFrame = None):
        """
        learn data processing parameters
        Parameters
        ----------
        df : pd.DataFrame
            When we fit and process data with processor one by one. The fit function reiles on the output of previous
            processor, i.e. `df`.
        """

    @abc.abstractmethod
    def __call__(self, df: pd.DataFrame):
        """
        process the data
        NOTE: **The processor could change the content of `df` inplace !!!!! **
        User should keep a copy of data outside
        Parameters
        ----------
        df : pd.DataFrame
            The raw_df of handler or result from previous processor.
        """


class CSFilter(Processor):
    """Outlier filter"""

    def __init__(self, target_cols=None, method="mad"):
        self.target_cols = target_cols
        if method == "mad":
            self.filter_func = mad_filter
        else:
            raise NotImplementedError(f"This type of method is not supported")

    def __call__(self, df):
        df[self.target_cols] = df[self.target_cols].groupby('Date', group_keys=False).apply(self.filter_func)
        return df


class CSFillna(Processor):
    """Cross Sectional Fill Nan"""

    def __init__(self, target_cols=None):
        self.target_cols = target_cols

    def __call__(self, df):
        df[self.target_cols] = df[self.target_cols].groupby('Date', group_keys=False).apply(lambda x: x.fillna(x.mean()))
        return df
