import abc

import pandas as pd

header = ["Symbol", "Date", "Open", "Close", "High", "Low", "Pre_Close", "Change", "Pct_Chg", "Volume", "AMount",
          "Turnover_rate", "Turnover_rate_f", "Volume_ratio", "Pe", "Pe_ttm", 'Pb', 'Ps', 'Ps_ttm', 'Dv_ratio',
          'Dv_ttm', 'Total_share', 'Float_share', 'Free_share', 'Total_mv', 'Circ_mv', 'Adj_factor']


class DataLoader(abc.ABC):
    @staticmethod
    def load_features(db_conn, symbol, timestamp_col='Date', start_time=None, end_time=None) -> pd.DataFrame:
        # 查询数据
        features = []
        with db_conn.cursor() as cursor:
            start_time = start_time.replace('-', '')
            end_time = end_time.replace('-', '')
            query = "SELECT daily.*, daily_basic.turnover_rate, daily_basic.turnover_rate_f, " \
                    "daily_basic.volume_ratio, daily_basic.pe, daily_basic.pe_ttm, " \
                    "daily_basic.pb, daily_basic.ps, daily_basic.ps_ttm, daily_basic.dv_ratio, " \
                    "daily_basic.dv_ttm, daily_basic.total_share, daily_basic.float_share, daily_basic.free_share, " \
                    "daily_basic.total_mv, daily_basic.circ_mv, factor.adj_factor " \
                    "FROM ts_quotation_daily daily " \
                    "JOIN ts_quotation_daily_basic daily_basic ON " \
                    "daily.ts_code=daily_basic.ts_code AND " \
                    "daily.trade_date=daily_basic.trade_date " \
                    "JOIN ts_quotation_adj_factor factor ON " \
                    "daily.ts_code=factor.ts_code AND " \
                    "daily.trade_date=factor.trade_date " \
                    "WHERE daily.ts_code='%s' AND daily.trade_date >= '%s' AND daily.trade_date <= '%s'" % (
                        symbol, start_time, end_time)

            cursor.execute(query)
            for row in cursor:
                feature = list(row)
                t_date = feature[1]
                feature[1] = t_date[0:4] + '-' + t_date[4:6] + '-' + t_date[6:8]
                features.append(feature)

        df = pd.DataFrame(features, columns=header)
        df = df.sort_values(by=timestamp_col, ascending=True)
        return df

    @staticmethod
    def load_symbols(db_conn, instruments):
        symbols = []
        with db_conn.cursor() as cursor:
            # 查询指数内的股票代码
            query = "SELECT DISTINCT(ts_code) FROM ts_idx_index_weight WHERE index_code='%s' " \
                    "AND trade_date in (SELECT MAX(trade_date) FROM ts_idx_index_weight WHERE index_code='%s')" % (
                        instruments, instruments)
            cursor.execute(query)
            for row in cursor:
                list_row = list(row)
                symbols.append(list_row[0])
        return symbols
