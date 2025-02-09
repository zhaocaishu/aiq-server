import pandas as pd

HEADER = [
    "Instrument",
    "Date",
    "Open",
    "Close",
    "High",
    "Low",
    "Pre_Close",
    "Change",
    "Pct_Chg",
    "Volume",
    "AMount",
    "Turnover_rate",
    "Turnover_rate_f",
    "Volume_ratio",
    "Pe",
    "Pe_ttm",
    "Pb",
    "Ps",
    "Ps_ttm",
    "Dv_ratio",
    "Dv_ttm",
    "Total_share",
    "Float_share",
    "Free_share",
    "Total_mv",
    "Circ_mv",
    "Adj_factor",
]


class DataLoader:
    @staticmethod
    def load_features(
        db_connection, instrument, timestamp_col="Date", start_time=None, end_time=None
    ) -> pd.DataFrame:
        # 查询数据
        features = []
        with db_connection.cursor() as cursor:
            start_time = start_time.replace("-", "")
            end_time = end_time.replace("-", "")
            query = (
                "SELECT daily.*, daily_basic.turnover_rate, daily_basic.turnover_rate_f, "
                "daily_basic.volume_ratio, daily_basic.pe, daily_basic.pe_ttm, "
                "daily_basic.pb, daily_basic.ps, daily_basic.ps_ttm, daily_basic.dv_ratio, "
                "daily_basic.dv_ttm, daily_basic.total_share, daily_basic.float_share, daily_basic.free_share, "
                "daily_basic.total_mv, daily_basic.circ_mv, factor.adj_factor "
                "FROM ts_quotation_daily daily "
                "JOIN ts_quotation_daily_basic daily_basic ON "
                "daily.ts_code=daily_basic.ts_code AND "
                "daily.trade_date=daily_basic.trade_date "
                "JOIN ts_quotation_adj_factor factor ON "
                "daily.ts_code=factor.ts_code AND "
                "daily.trade_date=factor.trade_date "
                "WHERE daily.ts_code='%s' LIMIT 50000" % instrument
            )

            cursor.execute(query)
            for row in cursor:
                feature = list(row)
                t_date = feature[1]
                feature[1] = t_date[0:4] + "-" + t_date[4:6] + "-" + t_date[6:8]
                features.append(feature)

        df = pd.DataFrame(features, columns=HEADER)
        df = df.sort_values(by=timestamp_col, ascending=True)
        return df
