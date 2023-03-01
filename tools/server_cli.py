import datetime
import json

from flask import Flask, request

from aiq.dataset import Alpha158
from aiq.models import XGBModel
from aiq.utils.logging import get_logger

from ais.strategies.signal_strategy import TopkDropoutStrategy
from ais.data.dataset import Dataset


app = Flask(__name__)
logger = get_logger('Aiq Server')

# load model
model = XGBModel()
model.load('/home/zcs/darrenwang/aiq-server/checkpoints')

# strategy
strategy = TopkDropoutStrategy()


@app.route("/predict", methods=['GET'])
def predict():
    request_dict = request.args.to_dict()
    tradeDate = request_dict['tradeDate']
    if 'curPosition' in request_dict:
        curPosition = request_dict['curPosition']
    else:
        curPosition = ''
    logger.info('input request: trade date: %s, current position: %s' % (tradeDate, curPosition))
    # input data
    start_time = datetime.datetime.strftime(
        datetime.datetime.strptime(tradeDate, '%Y-%m-%d') - datetime.timedelta(days=72), '%Y-%m-%d')
    end_time = tradeDate
    dataset = Dataset(instruments='000852.SH', start_time=start_time, end_time=end_time, min_periods=72,
                      handler=Alpha158(test_mode=True))
    logger.info('predict %d items' % dataset.to_dataframe().shape[0])

    # predict
    prediction_result = model.predict(dataset).to_dataframe()

    # response
    strategy.set_current_stock_list(curPosition)
    buy_order_list, keep_order_list, sell_order_list = strategy.generate_trade_decision(prediction_result)
    response = {
        "code": 0,
        "msg": "OK",
        "data": {
            'date': dataset.latest_date,
            'buy': buy_order_list,
            'sell': sell_order_list
        }
    }
    return json.dumps(response)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9000)
