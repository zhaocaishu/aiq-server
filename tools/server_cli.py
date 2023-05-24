import datetime
import json

import mysql.connector
from flask import Flask, request


from aiq.dataset import Alpha158, Alpha101
from aiq.models import XGBModel
from aiq.utils.logging import get_logger

from ais.strategies.signal_strategy import TopkDropoutStrategy
from ais.data.dataset import Dataset


app = Flask(__name__)
logger = get_logger('Aiq Server')

# build db connection
db_connection = mysql.connector.connect(host='127.0.0.1', user='zcs', passwd='mydaydayup2023!', database="stock_info")

# load model
model = XGBModel()
model.load('/home/zcs/darrenwang/aiq-server/checkpoints')

# strategy
strategy = TopkDropoutStrategy()


def to_trade_day(input_date):
    trade_day = None
    input_date = input_date.replace('-', '')
    with db_connection.cursor() as cursor:
        query = "SELECT min(cal_date) FROM ts_basic_trade_cal WHERE cal_date >= '%s' and exchange = 'SSE' and is_open " \
                "= 1" % input_date
        cursor.execute(query)
        for row in cursor:
            trade_day = row[0]
    return trade_day


@app.route("/predict", methods=['GET'])
def predict():
    # request
    request_dict = request.args.to_dict()
    tradeDate = request_dict['tradeDate']
    if 'curPosition' in request_dict:
        curPosition = request_dict['curPosition']
    else:
        curPosition = ''
    logger.info('input request: trade date: %s, current position: %s' % (tradeDate, curPosition))

    # build dataset
    start_time = datetime.datetime.strftime(
        datetime.datetime.strptime(tradeDate, '%Y-%m-%d') - datetime.timedelta(days=120), '%Y-%m-%d')
    end_time = to_trade_day(tradeDate)
    handlers = (Alpha158(test_mode=True), Alpha101(test_mode=True))
    dataset = Dataset(connection=db_connection, instruments='000852.SH', start_time=start_time, end_time=end_time,
                      handlers=handlers)
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
            'date': end_time,
            'buy': buy_order_list,
            'sell': sell_order_list
        }
    }
    return json.dumps(response)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9000)
