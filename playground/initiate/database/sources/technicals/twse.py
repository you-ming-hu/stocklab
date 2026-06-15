from ..base import Source
from ... import schema

import json
import pandas as pd

class Stocks(Source):
    
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

        for name in [cols['代號'], cols['名稱']]:
            df[name] = df[name].str.replace(' ','').replace('*','')

        for name in [cols['交易股數'], cols['交易筆數'], cols['交易金額']]:
            df[name] = df[name].str.replace(',','').astype(int)

        for name in [cols['開盤價'], cols['最高價'], cols['最低價'], cols['收盤價']]:
            df[name] = df[name].str.replace(',','').replace('--',pd.NA).astype(float)
        return df
    
    def add_other_columns(self, df):
        df[self.table.f_stock_info.市場別] = 'TWSE'
        return df

stocks = Stocks(
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
)