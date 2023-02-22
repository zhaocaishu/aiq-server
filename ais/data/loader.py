import abc

import pandas as pd


header = ['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Pre_Close', 'Change', 'Pct_Chg', 'Volume',
          'AMount', 'Turnover_rate', 'Turnover_rate_f', 'Volume_ratio', 'Pe', 'Pe_ttm', 'Pb', 'Ps',
          'Ps_ttm', 'Dv_ratio', 'Dv_ttm', 'Total_share', 'Float_share', 'Free_share', 'Total_mv',
          'Circ_mv']


class DataLoader(abc.ABC):
    @staticmethod
    def load(db_conn, symbol, timestamp_col='Date', start_time=None, end_time=None) -> pd.DataFrame:
        # 查询数据
        with db_conn.cursor() as cursor:
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
                        symbol, start_time, end_time)

            cursor.execute(query)
            features = []
            for row in cursor:
                feature = list(row)
                t_date = feature[1]
                feature[1] = t_date[0:4] + '-' + t_date[4:6] + '-' + t_date[6:8]
                features.append(feature)

            df = pd.DataFrame(features, columns=header)
            df = df.sort_values(by=timestamp_col, ascending=True)
            return df
