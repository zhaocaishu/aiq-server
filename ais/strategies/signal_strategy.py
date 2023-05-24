import abc
import datetime

import pandas as pd


class TopkDropoutStrategy(abc.ABC):
    def __init__(self, connection):
        """
        Top-k dropout strategy
        """
        self.topk = 30
        self.n_drop = 10
        self.hold_thresh = 1
        self.method_sell = 'bottom'
        self.method_buy = 'top'
        self.trade_day_interval = 5

        self.db_connection = connection

        # 当前持仓股票列表
        self.current_trade_date = None
        self.current_stock_list = []

    def set_current_stock_list(self, cur_position):
        cur_position = cur_position.strip()
        if len(cur_position) > 0:
            self.current_stock_list = cur_position.split(',')
        else:
            self.current_trade_date = None
            self.current_stock_list = []

    def trade_day_count(self, start_date, end_date):
        day_count = 0
        start_date = start_date.replace('-', '')
        end_date = end_date.replace('-', '')
        with self.db_connection.cursor() as cursor:
            query = "SELECT cal_date FROM ts_basic_trade_cal WHERE cal_date >= '%s' and cal_date <= '%s' and exchange " \
                    "= 'SSE' and is_open = 1" % (start_date, end_date)
            cursor.execute(query)
            for _ in cursor:
                day_count += 1
        return day_count

    def generate_trade_decision(self, trade_date, df_prediction):
        # generate order list for this adjust date
        sell_order_list = []
        buy_order_list = []

        # adjust stocks every n days
        if self.current_trade_date is not None:
            day_cnt = self.trade_day_count(self.current_trade_date, trade_date)
            if day_cnt <= self.trade_day_interval:
                return sell_order_list, buy_order_list

        # set current trade date
        self.current_trade_date = trade_date

        def get_first_n(li, n):
            return list(li)[:n]

        def get_last_n(li, n):
            return list(li)[-n:]

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

        # Get current stock list and trade date
        self.current_stock_list = list(set(self.current_stock_list) - set(sell_order_list)) + buy_order_list

        return buy_order_list, sell_order_list
