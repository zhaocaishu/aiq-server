import abc
import os

import numpy as np
import pandas as pd

import mysql.connector

from loader import DataLoader


class Dataset(abc.ABC):
    """
    Preparing data for model training and inference.
    """

    def __init__(
        self,
        instruments,
        start_time=None,
        end_time=None,
        min_periods=30,
        handler=None,
        shuffle=False
    ):
        self.connection = mysql.connector.connect(host='127.0.0.1', user='zcs', passwd='mydaydayup2023!',
                                                  database="stock_info")

        self.symbols = self.get_symbols(instruments)

        df_list = []
        for symbol in self.symbols:
            df = DataLoader.load(db_conn=self.connection, symbol=symbol, start_time=start_time, end_time=end_time)

            # skip ticker of non-existed or small periods
            if df is None or df.shape[0] < min_periods:
                continue

            # append ticker symbol
            df['Symbol'] = symbol

            # extract ticker factors
            if handler is not None:
                df = handler.fetch(df)

            df = df.iloc[-1]

            df_list.append(df)
        # concat and reset index
        self.df = pd.concat(df_list)
        self.df.reset_index(inplace=True)
        print('Loaded %d symbols to build dataset' % len(df_list))

        # random shuffle
        if shuffle:
            self.df = self.df.sample(frac=1)

    def get_symbols(self, instruments):
        symbols = []
        with self.connection.cursor() as cursor:
            # 查询指数内的股票代码
            query = "SELECT DISTINCT ts_code FROM ts_idx_index_weight WHERE index_code=%s" % instruments
            cursor.execute(query)
            for row in cursor:
                list_row = list(row)
                self.symbols.append(list_row[0])
        return symbols

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
