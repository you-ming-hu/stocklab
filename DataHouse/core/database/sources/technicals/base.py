from ... import schema

import pandas as pd

class STOCKS:

    def add_other_columns(self, df):
        df[schema.tables.StockDaily.f_stock_info.市場別] = self.__class__.market_type
        df[schema.tables.StockDaily.f_stock_info.交易中] = True
        return df

    def format_dtype(self, df):
        str_cols= [
            schema.tables.StockDaily.f_stock_info.代號,
            schema.tables.StockDaily.f_stock_info.名稱
        ]
        int_cols = [
            schema.tables.StockDaily.f_technicals_volume.交易股數,
            schema.tables.StockDaily.f_technicals_volume.交易筆數,
            schema.tables.StockDaily.f_technicals_volume.交易金額
        ]
        float_cols = [
            schema.tables.StockDaily.f_technicals_price.開盤價,
            schema.tables.StockDaily.f_technicals_price.最高價,
            schema.tables.StockDaily.f_technicals_price.最低價,
            schema.tables.StockDaily.f_technicals_price.收盤價,
        ]
        df = super().format_dtype(df, str_cols, int_cols, float_cols)
        df.loc[(df[float_cols] == 0).any(axis=1), int_cols+float_cols] = pd.NA
        return df
    