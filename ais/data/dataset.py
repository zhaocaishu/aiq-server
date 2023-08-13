import abc

import numpy as np
import pandas as pd

from ais.data.loader import DataLoader
from ais.data.processor import CSFillna, CSFilter


class Dataset(abc.ABC):
    """
    Preparing data for model training and inference.
    """

    def __init__(
        self,
        connection,
        instruments,
        start_time=None,
        end_time=None,
        handlers=None,
        adjust_price=True
    ):
        dfs = []
        ts_handler, cs_handler = handlers
        self.symbols = DataLoader.load_symbols(db_conn=connection, instruments=instruments)
        for symbol in self.symbols:
            df = DataLoader.load_features(db_conn=connection, symbol=symbol, start_time=start_time,
                                          end_time=end_time)

            # skip ticker of non-existed
            if df is None: continue

            # append ticker symbol
            df['Symbol'] = symbol

            # adjust price with factor
            if adjust_price:
                df = self.adjust_price(df)

            # extract ticker factors
            if ts_handler is not None:
                df = ts_handler.fetch(df)

            dfs.append(df)

        # concat dataframes and set index
        self.df = pd.concat(dfs, ignore_index=True)
        self.df = self.df.set_index(['Date', 'Symbol'])

        feature_names = []
        if ts_handler is not None:
            feature_names += ts_handler.feature_names

        # handler for cross-sectional factor
        if cs_handler is not None:
            self.df = cs_handler.fetch(self.df)
            feature_names += cs_handler.feature_names

        # current data
        self.df = self.df.loc[[end_time]]

        # processors
        if feature_names is not None:
            processors = [
                CSFillna(target_cols=feature_names),
                CSFilter(target_cols=feature_names)
            ]

            for processor in processors:
                self.df = processor(self.df)

        # reset index
        self.df.reset_index(inplace=True)

    @staticmethod
    def adjust_price(df):
        price_cols = ['Open', 'High', 'Low', 'Close']
        for col in price_cols:
            df[col] = df[col] * df['Adj_factor']
        return df

    def to_dataframe(self):
        return self.df

    def add_column(self, name: str, data: np.array):
        self.df[name] = data
