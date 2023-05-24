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
strategy = TopkDropoutStrategy(connection=db_connection)


def is_trade_day(input_date):
    is_trade = False
    input_date = input_date.replace('-', '')
    with db_connection.cursor() as cursor:
        query = "SELECT cal_date FROM ts_basic_trade_cal WHERE cal_date = '%s' and exchange = 'SSE' and is_open " \
                "= 1" % input_date
        cursor.execute(query)
        for _ in cursor:
            is_trade = True
    return is_trade


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

    # whether a trading day
    if is_trade_day(tradeDate):
        response = {
            "code": 1,
            "msg": "Date: %s is not a trading day" % tradeDate,
            "data": {}
        }
        return json.dumps(response)

    # build dataset
    start_time = datetime.datetime.strftime(
        datetime.datetime.strptime(tradeDate, '%Y-%m-%d') - datetime.timedelta(days=120), '%Y-%m-%d')
    end_time = tradeDate
    logger.info('input start time: %s, end time: %s' % (start_time, end_time))

    handlers = (Alpha158(test_mode=True), Alpha101(test_mode=True))
    dataset = Dataset(connection=db_connection, instruments='000852.SH', start_time=start_time, end_time=end_time,
                      handlers=handlers)

    if dataset.to_dataframe().shape[0] <= 0:
        response = {
            "code": 2,
            "msg": "Data not exists at date: %s" % tradeDate,
            "data": {}
        }
        return json.dumps(response)
    else:
        logger.info('predict %d items' % dataset.to_dataframe().shape[0])

    # predict
    prediction_result = model.predict(dataset).to_dataframe()

    # response
    strategy.set_current_stock_list(curPosition)
    buy_order_list, sell_order_list = strategy.generate_trade_decision(end_time, prediction_result)
    response = {
        "code": 0,
        "msg": "OK",
        "data": {
            'date': tradeDate,
            'buy': buy_order_list,
            'sell': sell_order_list
        }
    }
    return json.dumps(response)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9000)
