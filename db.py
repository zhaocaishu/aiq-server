import datetime

import mysql.connector

import pandas as pd


class DBManager(object):
    def __init__(self):
        """init db, and show tables"""
        self.connection = mysql.connector.connect(host='127.0.0.1', user='zcs', passwd='mydaydayup2023!',
                                                  database="stock_info")
        self.header = ["Symbol", "Date", "Open", "High", "Low", "Close", "Pre_Close", "Change", "Pct_Chg", "Volume",
                       "AMount", "Turnover_rate", "Turnover_rate_f", "Volume_ratio", "Pe", "Pe_ttm", 'Pb', 'Ps',
                       'Ps_ttm', 'Dv_ratio', 'Dv_ttm', 'Total_share', 'Float_share', 'Free_share', 'Total_mv',
                       'Circ_mv']

    def fetch_data(self, cur_date):
        # 从数据库导出数据
        with self.connection.cursor() as cursor:
            # 查询指数内的股票代码
            query = "SELECT DISTINCT ts_code FROM ts_idx_index_weight WHERE index_code='000300.SH'"

            cursor.execute(query)

            codes = []
            for row in cursor:
                list_row = list(row)
                codes.append(list_row[0])

            # 查询股票代码在时间范围内的特征
            begin_date = datetime.datetime.strftime(
                datetime.datetime.strptime(cur_date, '%Y-%m-%d') - datetime.timedelta(days=60), '%Y-%m-%d')
            end_date = cur_date
            for code in codes:
                # 查询数据
                query = "SELECT daily.*, daily_basic.turnover_rate, daily_basic.turnover_rate_f, " \
                        "daily_basic.volume_ratio, daily_basic.pe, daily_basic.pe_ttm, " \
                        "daily_basic.pb, daily_basic.ps, daily_basic.ps_ttm, daily_basic.dv_ratio, " \
                        "daily_basic.dv_ttm, daily_basic.total_share, daily_basic.float_share, daily_basic.free_share, " \
                        "daily_basic.total_mv, daily_basic.circ_mv " \
                        "FROM ts_quotation_daily daily " \
                        "JOIN ts_quotation_daily_basic daily_basic ON " \
                        "daily.ts_code=daily_basic.ts_code AND " \
                        "daily.trade_date=daily_basic.trade_date " \
                        "WHERE daily.ts_code='%s' AND daily.trade_date >= '%s' AND  daily.trade_date <= '%s'" % (
                            code, begin_date, end_date)

                cursor.execute(query)
                features = []
                for row in cursor:
                    feature = list(row)
                    t_date = feature[1]
                    feature[1] = t_date[0:4] + '-' + t_date[4:6] + '-' + t_date[6:8]
                    features.append(feature)

            return pd.DataFrame(features, columns=self.header)
