import datetime
import numpy as np

import uvicorn
from fastapi import FastAPI
import mysql.connector

from aic.data.loader import DataLoader
from aic.utils.logging import get_logger


# app
app = FastAPI()


@app.get("/predict")
async def predict(tradeDate: str, instrument: str):
    global logger
    # request
    logger.info(
        "input request: trade date: %s, instrument: %s" % (tradeDate, instrument)
    )

    # open db connection
    db_connection = mysql.connector.connect(
        host="127.0.0.1", user="zcs", passwd="2025zcsdaydayup", database="stock_info"
    )

    # build dataset
    start_time = datetime.datetime.strftime(
        datetime.datetime.strptime(tradeDate, "%Y-%m-%d")
        - datetime.timedelta(days=120),
        "%Y-%m-%d",
    )
    end_time = datetime.datetime.strftime(
        datetime.datetime.strptime(tradeDate, "%Y-%m-%d") + datetime.timedelta(days=30),
        "%Y-%m-%d",
    )
    logger.info("dataset: start time: %s, end time: %s" % (start_time, end_time))

    data = DataLoader.load_features(
        db_connection=db_connection,
        instrument=instrument,
        start_time=start_time,
        end_time=end_time,
    )

    logger.info("Dataset: loaded %d items" % data.shape[0])

    # close db connection
    db_connection.close()

    # prediction
    preds = np.random.randint(0, 21, 5)
    probs = np.random.rand(5, 21)
    logger.info("Model prediction successfully")

    # response
    response = {
        "code": 0,
        "msg": "OK",
        "data": {"date": tradeDate, "preds": preds, "probs": probs},
    }
    logger.info("Reponse: %s" % str(response))

    return response


if __name__ == "__main__":
    # logger
    logger = get_logger("AIQ-Cli")

    # run
    uvicorn.run(app, host="0.0.0.0", port=9000, workers=1)
