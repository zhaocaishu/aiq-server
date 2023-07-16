import datetime
from typing import Optional

import mysql.connector
import uvicorn
from fastapi import FastAPI

from aiq.dataset import Alpha158, Alpha101
from aiq.models import XGBModel
from aiq.utils.logging import get_logger

from ais.strategies.signal_strategy import TopkDropoutStrategy
from ais.data.dataset import Dataset
from ais.utils.trade_exchange import TradeExchange

# trade per interval days
TRADE_INTERVAL_DAYS = 5

# app
app = FastAPI()


@app.get("/predict")
async def predict(tradeDate: str, curPosition: Optional[str] = ''):
    global logger, model, strategy
    # request
    logger.info('input request: trade date: %s, current position: %s' % (tradeDate, curPosition))

    # open db connection
    db_connection = mysql.connector.connect(host='127.0.0.1', user='zcs', passwd='mydaydayup2023!',
                                            database="stock_info")
    trade_exchange = TradeExchange(connection=db_connection)
    logger.info('trade exchange: db connection and trade exchange started')

    # check tradable date
    if not trade_exchange.is_tradable_day(tradeDate):
        # response
        response = {
            "code": 1,
            "msg": "%s is not a tradable day" % tradeDate,
            "data": {}
        }

        # close db connection
        db_connection.close()

        return response

    # set current positions
    strategy.set_current_stock_list(curPosition)

    # check trade interval days
    if strategy.current_trade_date is not None:
        interval_days = trade_exchange.get_trade_day_intervals(strategy.current_trade_date, tradeDate)
        if interval_days <= TRADE_INTERVAL_DAYS:
            # response
            response = {
                "code": 1,
                "msg": "%s is not a tradable day" % tradeDate,
                "data": {}
            }

            # close db connection
            db_connection.close()

            return response

    # build dataset
    start_time = datetime.datetime.strftime(
        datetime.datetime.strptime(tradeDate, '%Y-%m-%d') - datetime.timedelta(days=120), '%Y-%m-%d')
    end_time = trade_exchange.get_trade_date(tradeDate)
    logger.info('dataset: start time: %s, end time: %s' % (start_time, end_time))

    handlers = (Alpha158(test_mode=True), Alpha101(test_mode=True))
    dataset = Dataset(connection=db_connection, instruments='000852.SH', start_time=start_time, end_time=end_time,
                      handlers=handlers)
    logger.info('dataset: loaded %d items' % dataset.to_dataframe().shape[0])

    # predict
    prediction_result = model.predict(dataset).to_dataframe()
    logger.info('model: predicted successfully')

    # response
    buy_order_list, sell_order_list = strategy.generate_trade_decision(tradeDate, prediction_result)
    response = {
        "code": 0,
        "msg": "OK",
        "data": {
            'date': tradeDate,
            'buy': buy_order_list,
            'sell': sell_order_list
        }
    }
    logger.info('strategy: trade decision has been generated')

    # close db connection
    db_connection.close()

    return response


if __name__ == '__main__':
    # logger
    logger = get_logger('Aiq Service')

    # model
    model = XGBModel()
    model.load('/home/zcs/darrenwang/aiq-server/checkpoints')

    # strategy
    strategy = TopkDropoutStrategy()

    # run
    uvicorn.run(app, host='0.0.0.0', port=9000, workers=1)
