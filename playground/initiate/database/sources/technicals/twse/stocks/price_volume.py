from ...base import STOCKS
from ..... import schema

import pandas as pd
import re

class VERSION_0(STOCKS):
    market_type = 'TWSE'
    
    def check_empty(self, content):
        return not 'tables' in content
    
    def to_df(self, content):
        target_table = None
        for table in content['tables']:
            if '每日收盤行情' in table.get('title',''):
                target_table = table
        assert target_table is not None
        df = pd.DataFrame(
            target_table['data'],
            columns=target_table['fields'],
        )
        return df
        
    def format_dtype(self, df):
        stock_info_cols= [
            schema.tables.StockDaily.f_stock_info.代號,
            schema.tables.StockDaily.f_stock_info.名稱
        ]
        volume_cols = [
            schema.tables.StockDaily.f_technicals_volume.交易股數,
            schema.tables.StockDaily.f_technicals_volume.交易筆數,
            schema.tables.StockDaily.f_technicals_volume.交易金額
        ]
        price_cols = [
            schema.tables.StockDaily.f_technicals_price.開盤價,
            schema.tables.StockDaily.f_technicals_price.最高價,
            schema.tables.StockDaily.f_technicals_price.最低價,
            schema.tables.StockDaily.f_technicals_price.收盤價,
        ]

        for name in stock_info_cols:
            df[name] = df[name].str.replace(' ','').replace('*','')

        for name in volume_cols:
            df[name] = df[name].str.replace(',','').astype(int)

        for name in price_cols:
            df[name] = df[name].map(lambda t: re.sub(r'[^0-9.]','',t)).replace('',pd.NA).astype(float)
        
        df = df.loc[~(df[volume_cols+price_cols] == 0).all(axis=1)]
        return df

version_0 = VERSION_0(
    schema.tables.StockDaily,
    {
        '證券代號': schema.tables.StockDaily.f_stock_info.代號, 
        '證券名稱': schema.tables.StockDaily.f_stock_info.名稱,
        '開盤價': schema.tables.StockDaily.f_technicals_price.開盤價,
        '最高價': schema.tables.StockDaily.f_technicals_price.最高價,
        '最低價': schema.tables.StockDaily.f_technicals_price.最低價,
        '收盤價': schema.tables.StockDaily.f_technicals_price.收盤價,
        '成交股數': schema.tables.StockDaily.f_technicals_volume.交易股數,
        '成交筆數': schema.tables.StockDaily.f_technicals_volume.交易筆數,
        '成交金額': schema.tables.StockDaily.f_technicals_volume.交易金額,
    },
    True
)