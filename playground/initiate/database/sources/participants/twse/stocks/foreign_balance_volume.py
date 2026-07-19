from ....base import Source
from ..... import schema

import json
import pandas as pd

class VERSION_0(Source):
    
    def open(self, file):
        with open(file, encoding="utf-8") as f:
            content = json.load(f)
        return content
    
    def check_empty(self, content):
        return content['data'] == []
    
    def to_df(self, content):
        df = pd.DataFrame(columns=content['fields'], data=content['data'])
        assert len(df.columns) == 11
        return df
        
    def format_dtype(self, df):
        cols = self.table.cols
        
        stock_info_cols = [cols['代號']]
        volume_cols = [cols['總發行股數'], cols['外陸資持有股數']]
        ratio_cols = [cols['外陸資投資上限比率']]

        for name in stock_info_cols:
            df[name] = df[name].str.replace(' ','').replace('*','')

        for name in volume_cols:
            df[name] = df[name].str.replace(',','').astype(int)

        for name in ratio_cols:
            df[name] = df[name].astype(float)

        return df

version_0 = VERSION_0(
    schema.tables.StockDaily,
    {
        '證券代號': schema.tables.StockDaily.f_stock_info.代號,
        '發行股數': schema.tables.StockDaily.f_stock_info.總發行股數,
        '全體外資持有股數': schema.tables.StockDaily.f_foreign_balance_volume.外陸資_持有_股數,
        '法令投資上限比率': schema.tables.StockDaily.f_foreign_limit.外陸資_投資上限_比率,
    },
    True
)

class VERSION_1(VERSION_0):
    
    def to_df(self, content):
        df = pd.DataFrame(columns=content['fields'], data=content['data'])
        assert len(df.columns) == 12
        return df

version_1 = VERSION_1(
    schema.tables.StockDaily,
    {
        '證券代號': schema.tables.StockDaily.f_stock_info.代號,
        '發行股數': schema.tables.StockDaily.f_stock_info.總發行股數,
        '全體外資及陸資持有股數': schema.tables.StockDaily.f_foreign_balance_volume.外陸資_持有_股數,
        '外資及陸資共用法令投資上限比率': schema.tables.StockDaily.f_foreign_limit.外陸資_投資上限_比率,
    },
    True
)