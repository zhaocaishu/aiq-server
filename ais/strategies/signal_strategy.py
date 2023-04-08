import abc


class TopkStrategy(abc.ABC):
    def __init__(self):
        """
        Top-k strategy
        """
        self.volume_thresh = 1.5
        self.close_thresh = 0.04

    def generate_trade_decision(self, df_prediction):
        valid = (df_prediction['VOLUME30'] > self.volume_thresh) & (df_prediction['CLOSE1'] > self.close_thresh)
        buy_order_list = df_prediction[valid]['Symbol']

        return buy_order_list
