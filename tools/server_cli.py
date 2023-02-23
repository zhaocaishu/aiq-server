import datetime
import json

from flask import Flask

from aiq.dataset import Alpha158
from aiq.models import XGBModel
from aiq.utils.logging import get_logger

from ais.strategies.signal_strategy import TopkDropoutStrategy
from ais.data.dataset import Dataset


app = Flask(__name__)
logger = get_logger('Aiq Server')

# load model
model = XGBModel()
model.load('/home/zcs/darrenwang/aiq/checkpoints')

# strategy
strategy = TopkDropoutStrategy()


@app.route("/predict/date=<date>")
def predict(date):
    logger.info('input request: %s' % date)
    # input data
    start_time = datetime.datetime.strftime(
        datetime.datetime.strptime(date, '%Y-%m-%d') - datetime.timedelta(days=100), '%Y-%m-%d')
    end_time = date
    dataset = Dataset(instruments='000300.SH', start_time=start_time, end_time=end_time, min_periods=60,
                      handler=Alpha158(test_mode=True), shuffle=False)
    logger.info('predict %d items' % dataset.to_dataframe().shape[0])

    # predict
    prediction_result = model.predict(dataset).to_dataframe()

    # response
    buy_order_list, keep_order_list, sell_order_list = strategy.generate_trade_decision(prediction_result)
    response = {'DATE': date, 'BUY': buy_order_list, 'SELL': sell_order_list}
    return json.dumps(response)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9000)
