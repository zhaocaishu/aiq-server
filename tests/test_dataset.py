from ais.data.handler import Alpha158

from ais.data.dataset import Dataset


if __name__ == '__main__':
    dataset = Dataset(start_time='2021-09-20', end_time='2022-02-20', min_periods=72, handler=Alpha158(test_mode=True))
    print(dataset.to_dataframe().head(1))
