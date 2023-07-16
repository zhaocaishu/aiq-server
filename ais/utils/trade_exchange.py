import datetime


class TradeExchange:
    def __init__(self, connection):
        self.connection = connection

    def is_tradable_day(self, input_date):
        input_date = input_date.replace('-', '')
        with self.connection.cursor() as cursor:
            query = "SELECT cal_date FROM ts_basic_trade_cal WHERE cal_date = '%s' and exchange = 'SSE' and is_open " \
                    "= 1" % input_date
            cursor.execute(query)
            rst = cursor.fetchone()
            is_tradable = True if rst is not None else False
        return is_tradable

    def get_trade_date(self, input_date):
        input_date = input_date.replace('-', '')
        with self.connection.cursor() as cursor:
            query = "SELECT MAX(cal_date) FROM ts_basic_trade_cal WHERE cal_date < '%s' and exchange = 'SSE' and " \
                    "is_open = 1" % input_date
            cursor.execute(query)
            rst = cursor.fetchone()
            last_trade_date = rst[0]
        last_trade_date = datetime.datetime.strftime(datetime.datetime.strptime(last_trade_date, '%Y%m%d'), '%Y-%m-%d')
        return last_trade_date

    def get_trade_day_intervals(self, start_date, end_date):
        start_date = start_date.replace('-', '')
        end_date = end_date.replace('-', '')
        with self.connection.cursor() as cursor:
            query = "SELECT COUNT(cal_date) FROM ts_basic_trade_cal WHERE cal_date >= '%s' and cal_date <= '%s' and " \
                    "exchange = 'SSE' and is_open = 1" % (start_date, end_date)
            cursor.execute(query)
            rst = cursor.fetchone()
            day_intervals = rst[0]
        return day_intervals
