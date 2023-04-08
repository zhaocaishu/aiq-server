import abc

import pandas as pd
import numpy as np

from aiq.ops import Ref, Mean


class DataHandler(abc.ABC):
    def __init__(self):
        pass

    def fetch(self, df: pd.DataFrame = None) -> pd.DataFrame:
        pass


class Alpha158(DataHandler):
    def __init__(self, test_mode=False):
        self.test_mode = test_mode

        self.feature_names_ = None
        self.label_name_ = None

    def fetch(self, df: pd.DataFrame = None) -> pd.DataFrame:
        close = df['Close']
        volume = df['Volume']

        features = [volume / Mean(Ref(volume, 1), 30),
                    close / Ref(close, 1) - 1,
                    Ref(close, -5) / Ref(close, -1) - 1]
        names = ['VOLUME30', 'CLOSE1', 'RETURN5']

        window_size = 5
        slopes = np.ones(close.shape[0], dtype=np.int32) * np.nan
        for i in range(close.shape[0] - window_size):
            window_prices = close.iloc[i:i + window_size].values
            slopes[i] = np.arctan(np.polyfit(np.arange(window_size), window_prices / window_prices[0], deg=1)[0])
        features.append(pd.Series(slopes))
        names.append('SLOPE5')

        # concat all features and labels
        df = pd.concat(
            [df, pd.concat([features[i].rename(names[i]) for i in range(len(names))], axis=1).astype('float32')],
            axis=1)

        return df

    @property
    def feature_names(self):
        return self.feature_names_

    @property
    def label_name(self):
        return self.label_name_
