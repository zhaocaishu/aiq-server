import abc


class TopkStrategy(abc.ABC):
    def __init__(self):
        """
        Top-k strategy
        """
        pass

    def generate_trade_decision(self, df, volume_thresh, close_thresh, return_thresh, slope_thresh):
        valid = (df['VOLUME30'] > volume_thresh) & (df['CLOSE1'] > close_thresh) & (df['RETURN5'] > return_thresh) & (
                    df['SLOPE5'] > slope_thresh)
        buy_order_list = df[valid]['Symbol'].to_list()

        return buy_order_list
