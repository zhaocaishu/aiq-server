from aiq.dataset.handler import Alpha158

from data.dataset import Dataset


if __name__ == '__main__':
    dataset = Dataset('000300.SH', start_time='2022-02-20', end_time='2022-02-20', min_periods=30,
                      handler=Alpha158(test_mode=True), shuffle=False)
    print(dataset.to_dataframe().head(1))
