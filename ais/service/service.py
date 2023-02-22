import datetime
import json

from flask import Flask

from aiq.dataset import Alpha158
from aiq.models import XGBModel, LGBModel
from aiq.utils.config import config as cfg

from ais.strategies.signal_strategy import TopkDropoutStrategy
from ais.data.dataset import Dataset


app = Flask(__name__)

# init config
cfg.from_file('/home/zcs/darrenwang/aiq/configs/xgboost_model_reg.yaml')

# load model
handler = Alpha158(test_mode=True)
if cfg.model.name == 'XGB':
    model = XGBModel(feature_cols=handler.feature_names,
                     label_col=handler.label_name,
                     model_params=cfg.model.params)
elif cfg.model.name == 'LGB':
    model = LGBModel(feature_cols=handler.feature_names,
                     label_col=handler.label_name,
                     model_params=cfg.model.params)
model.load('/home/zcs/darrenwang/aiq/checkpoints/model.json')

# strategy
strategy = TopkDropoutStrategy()


def get_prediction(dataset):
    result = model.predict(dataset).to_dataframe()
    return result


@app.route("/predict/date=<date>")
def predict(date):
    # input data
    start_time = datetime.datetime.strftime(
        datetime.datetime.strptime(date, '%Y-%m-%d') - datetime.timedelta(days=60), '%Y-%m-%d')
    end_time = date

    dataset = Dataset('000300.SH', start_time=start_time, end_time=end_time, min_periods=30,
                      handler=handler, shuffle=False)
    prediction_result = get_prediction(dataset)

    buy_order_list, keep_order_list, sell_order_list = strategy.generate_trade_decision(prediction_result)

    response = {'DATE': date, 'BUY': buy_order_list, 'SELL': sell_order_list}
    return json.dumps(response)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9000)
