import datetime
import json

from flask import Flask, request

from aiq.utils.logging import get_logger

from ais.strategies.signal_strategy import TopkStrategy
from ais.data.dataset import Dataset
from ais.data.handler import Alpha158

app = Flask(__name__)
logger = get_logger('Aiq Server')

# strategy
strategy = TopkStrategy()


@app.route("/predict", methods=['GET'])
def predict():
    # request
    request_dict = request.args.to_dict()
    tradeDate = request_dict['tradeDate']
    volumeThresh = request_dict.get('volumeThresh', 1.6)
    closeThresh = request_dict.get('closeThresh', 0.04)
    returnThresh = request_dict.get('returnThresh', 0.05)
    slopeThresh = request_dict.get('slopeThresh', 0.001)
    logger.info(
        'input request: trade date: %s, volume threshold: %s, close threshold: %s, return threshold: %s,'
        'slope threshold: %s' % (tradeDate, volumeThresh, closeThresh, returnThresh, slopeThresh))

    # input data
    start_time = datetime.datetime.strftime(
        datetime.datetime.strptime(tradeDate, '%Y-%m-%d') - datetime.timedelta(days=120), '%Y-%m-%d')
    end_time = tradeDate
    dataset = Dataset(instruments='000852.SH', start_time=start_time, end_time=end_time, min_periods=72,
                      handler=Alpha158(test_mode=True))
    logger.info('predict %d items' % dataset.to_dataframe().shape[0])

    # response
    buy_order_list = strategy.generate_trade_decision(dataset.to_dataframe(), volumeThresh, closeThresh, returnThresh,
                                                      slopeThresh)
    response = {
        "code": 0,
        "msg": "OK",
        "data": {
            'date': dataset.date,
            'buy': buy_order_list
        }
    }
    return json.dumps(response)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9000)
