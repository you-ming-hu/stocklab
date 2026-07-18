from ....base import Source
from ..... import schema

import json
import pandas as pd
import re

class PRICE_VOLUME(Source):
    
    def open(self, file):
        with open(file, encoding="utf-8") as f:
            content = json.load(f)
        return content
    
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
        cols = self.table.cols
        
        stock_info_cols= [cols['代號'], cols['名稱']]
        volume_cols = [cols['交易股數'], cols['交易筆數'], cols['交易金額']]
        price_cols = [cols['開盤價'], cols['最高價'], cols['最低價'], cols['收盤價']]

        for name in stock_info_cols:
            df[name] = df[name].str.replace(' ','').replace('*','')

        for name in volume_cols:
            df[name] = df[name].str.replace(',','').astype(int)

        for name in price_cols:
            df[name] = df[name].map(lambda t: re.sub(r'[^0-9.]','',t)).replace('',pd.NA).astype(float)
        
        df = df.loc[~(df[volume_cols+price_cols] == 0).all(axis=1)]
        
        return df
    
    def add_other_columns(self, df):
        df[self.table.f_stock_info.市場別] = 'TWSE'
        return df

price_volume = PRICE_VOLUME(
    schema.tables.StockDaily,
    {
        '證券代號': schema.tables.StockDaily.f_stock_info.代號, 
        '證券名稱': schema.tables.StockDaily.f_stock_info.名稱,
        '開盤價': schema.tables.StockDaily.f_techicals.開盤價,
        '最高價': schema.tables.StockDaily.f_techicals.最高價,
        '最低價': schema.tables.StockDaily.f_techicals.最低價,
        '收盤價': schema.tables.StockDaily.f_techicals.收盤價,
        '成交股數': schema.tables.StockDaily.f_techicals.交易股數,
        '成交筆數': schema.tables.StockDaily.f_techicals.交易筆數,
        '成交金額': schema.tables.StockDaily.f_techicals.交易金額,
    },
    True
)