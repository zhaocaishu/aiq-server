import abc

import pandas as pd

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
                    close / Ref(close, 1) - 1]
        names = ['VOLUME30', 'CLOSE1']

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
