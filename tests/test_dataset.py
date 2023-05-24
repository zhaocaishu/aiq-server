from aiq.dataset.handler import Alpha158, Alpha101

from ais.data.dataset import Dataset


if __name__ == '__main__':
    handlers = (Alpha158(test_mode=True), Alpha101(test_mode=True))
    dataset = Dataset('000300.SH', start_time='2021-09-20', end_time='2022-02-20', handlers=handlers)
    print(dataset.to_dataframe().head(1))
