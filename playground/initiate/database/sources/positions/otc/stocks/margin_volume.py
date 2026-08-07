from ..... import schema

from ..base import MARGIN_V0, MARGIN_V1

import pandas as pd

class VERSION_0(MARGIN_V0):
    def to_df(self, content):
        df, market = super().to_df(content)
        return df
        
    def format_dtype(self, df):
        str_cols= [
            schema.tables.StockDaily.f_stock_info.代號,
        ]
        int_cols = [
            schema.tables.StockDaily.f_margin_flow_volume.融資_買進_股數,
            schema.tables.StockDaily.f_margin_flow_volume.融資_賣出_股數,
            schema.tables.StockDaily.f_margin_flow_volume.融資_現償_股數,
            schema.tables.StockDaily.f_margin_balance_volume.融資_餘額_股數,
            schema.tables.StockDaily.f_margin_limit.融資_次日限額_股數
        ]
        df = super().format_dtype(df, str_cols, int_cols)
        df[int_cols] = df[int_cols] * 1000
        return df

version_0 = VERSION_0(
    schema.tables.StockDaily,
    {
        '代號': schema.tables.StockDaily.f_stock_info.代號,
        '融資買進': schema.tables.StockDaily.f_margin_flow_volume.融資_買進_股數,
        '融資賣出': schema.tables.StockDaily.f_margin_flow_volume.融資_賣出_股數,
        '現金償還': schema.tables.StockDaily.f_margin_flow_volume.融資_現償_股數,
        '本日融資餘額': schema.tables.StockDaily.f_margin_balance_volume.融資_餘額_股數,
        '限額': schema.tables.StockDaily.f_margin_limit.融資_次日限額_股數
    },
    True
)

class VERSION_1(MARGIN_V1):
    def to_df(self, content):
        content = super().to_df(content)
        df = pd.DataFrame(columns=content['fields'],data=content['data'])
        return df
    
    def format_dtype(self, df):
        str_cols= [
            schema.tables.StockDaily.f_stock_info.代號,
        ]
        int_cols = [
            schema.tables.StockDaily.f_margin_flow_volume.融資_買進_股數,
            schema.tables.StockDaily.f_margin_flow_volume.融資_賣出_股數,
            schema.tables.StockDaily.f_margin_flow_volume.融資_現償_股數,
            schema.tables.StockDaily.f_margin_balance_volume.融資_餘額_股數,
            schema.tables.StockDaily.f_margin_limit.融資_次日限額_股數
        ]
        df = super().format_dtype(df, str_cols, int_cols)
        df[int_cols] = df[int_cols] * 1000
        return df

version_1 = VERSION_1(
    schema.tables.StockDaily,
    {
        '代號': schema.tables.StockDaily.f_stock_info.代號,
        '資買': schema.tables.StockDaily.f_margin_flow_volume.融資_買進_股數,
        '資賣': schema.tables.StockDaily.f_margin_flow_volume.融資_賣出_股數,
        '現償': schema.tables.StockDaily.f_margin_flow_volume.融資_現償_股數,
        '資餘額': schema.tables.StockDaily.f_margin_balance_volume.融資_餘額_股數,
        '資限額': schema.tables.StockDaily.f_margin_limit.融資_次日限額_股數
    },
    True
)