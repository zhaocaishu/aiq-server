import abc

import numpy as np
import pandas as pd

import mysql.connector

from ais.data.loader import DataLoader


class Dataset(abc.ABC):
    """
    Preparing data for model training and inference.
    """

    def __init__(
        self,
        instruments,
        start_time=None,
        end_time=None,
        min_periods=None,
        handler=None,
        adjust_price=True
    ):
        self.connection = mysql.connector.connect(host='127.0.0.1', user='zcs', passwd='mydaydayup2023!',
                                                  database="stock_info")

        self.symbols = DataLoader.load_symbols(db_conn=self.connection, instruments=instruments)

        dfs = []
        for symbol in self.symbols:
            df = DataLoader.load_features(db_conn=self.connection, symbol=symbol, start_time=start_time,
                                          end_time=end_time)

            # skip ticker of non-existed or small periods
            if df is None or df.shape[0] < min_periods:
                continue

            # append ticker symbol
            df['Symbol'] = symbol

            # adjust price with factor
            if adjust_price:
                df = self.adjust_price(df)

            # extract ticker factors
            if handler is not None:
                df = handler.fetch(df)

            df = df.iloc[-1:]
            dfs.append(df)

        self.df = pd.concat(dfs)
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

    @property
    def date(self):
        return self.df['Date'].iloc[0]
