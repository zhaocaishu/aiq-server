import abc
import os

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
        min_periods=60,
        handler=None,
        shuffle=False
    ):
        self.connection = mysql.connector.connect(host='127.0.0.1', user='zcs', passwd='mydaydayup2023!',
                                                  database="stock_info")

        self.symbols = DataLoader.load_symbols(db_conn=self.connection, instruments=instruments, start_time=start_time,
                                               end_time=end_time)

        self._latest_date = None

        df_list = []
        for symbol in self.symbols:
            df = DataLoader.load_features(db_conn=self.connection, symbol=symbol, start_time=start_time,
                                          end_time=end_time)

            # skip ticker of non-existed or small periods
            if df is None or df.shape[0] < min_periods:
                continue

            # append ticker symbol
            df['Symbol'] = symbol

            # extract ticker factors
            if handler is not None:
                df = handler.fetch(df)

            df = df.iloc[-1:]

            if self._latest_date is None:
                self._latest_date = df['Date'].values[0]

            df_list.append(df)
        # concat and reset index
        self.df = pd.concat(df_list)
        self.df.reset_index(inplace=True)
        print('Loaded %d symbols to build dataset' % len(df_list))

        # random shuffle
        if shuffle:
            self.df = self.df.sample(frac=1)

    def to_dataframe(self):
        return self.df

    def add_column(self, name: str, data: np.array):
        self.df[name] = data

    def dump(self, output_dir: str = None):
        if output_dir is None:
            return

        if not os.path.exists(path=output_dir):
            os.makedirs(output_dir)

        for symbol in self.symbols:
            df_symbol = self.df[self.df['Symbol'] == symbol]
            if df_symbol.shape[0] > 0:
                df_symbol.to_csv(os.path.join(output_dir, symbol + '.csv'), na_rep='NaN', index=False)

    @property
    def latest_date(self):
        return self._latest_date
