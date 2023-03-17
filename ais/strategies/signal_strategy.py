import abc

import pandas as pd


class TopkDropoutStrategy(abc.ABC):
    def __init__(self):
        """
        Top-k dropout strategy
        """
        self.topk = 30
        self.n_drop = 3
        self.hold_thresh = 1
        self.method_sell = 'bottom'
        self.method_buy = 'top'

        # 当前持仓股票列表
        self.current_stock_list = []

    def set_current_stock_list(self, cur_position):
        cur_position = cur_position.strip()
        if len(cur_position) > 0:
            self.current_stock_list = cur_position.split(',')
        else:
            self.current_stock_list = []

    def generate_trade_decision(self, df_prediction):
        def get_first_n(li, n):
            return list(li)[:n]

        def get_last_n(li, n):
            return list(li)[-n:]

        # generate order list for this adjust date
        sell_order_list = []
        keep_order_list = []
        buy_order_list = []

        # load score
        pred_score = pd.DataFrame({'score': df_prediction['PREDICTION'].tolist()},
                                  index=df_prediction['Symbol'].tolist())

        # last position (sorted by score)
        last = pred_score.reindex(self.current_stock_list).sort_values(by='score', ascending=False).index

        # The new stocks today want to buy **at most**
        if self.method_buy == "top":
            today = get_first_n(
                pred_score[~pred_score.index.isin(last)].sort_values(by='score', ascending=False).index,
                self.n_drop + self.topk - len(last),
            )
        else:
            raise NotImplementedError(f"This type of input is not supported")

        # combine(new stocks + last stocks),  we will drop stocks from this list
        # In case of dropping higher score stock and buying lower score stock.
        comb = pred_score.reindex(last.union(pd.Index(today))).sort_values(by='score', ascending=False).index

        # Get the stock list we really want to sell (After filtering the case that we sell high and buy low)
        if self.method_sell == "bottom":
            sell = last[last.isin(get_last_n(comb, self.n_drop))]
        else:
            raise NotImplementedError(f"This type of input is not supported")

        # Get the stock list we really want to buy and sell
        buy = today[:len(sell) + self.topk - len(last)]
        for code in buy:
            buy_order_list.append(code)

        for code in self.current_stock_list:
            if code in sell:
                sell_order_list.append(code)
            else:
                keep_order_list.append(code)

        # Get current stock list
        self.current_stock_list = buy_order_list + keep_order_list

        return buy_order_list, keep_order_list, sell_order_list
