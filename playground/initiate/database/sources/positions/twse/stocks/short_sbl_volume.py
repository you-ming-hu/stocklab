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
        head_cols = []
        i = 0
        for group in content['groups']:
            span = group['span']
            title = group['title']
            head_cols.extend([title+n for n in content['fields'][i:i+span]])
            i += span
        assert len(head_cols) == 14
        df = pd.DataFrame(columns=head_cols, data=content['data'])
        df = df.loc[df['股票代號']!='']

        return df
        
    def format_dtype(self, df):
        cols = self.table.cols
        
        stock_info_cols= [cols['代號']]
        volume_cols = [
            cols['融券賣出股數'], cols['融券買進股數'], cols['融券現償股數'], cols['融券餘額股數'], cols['融券次日限額股數'],
            cols['借券賣出賣出股數'], cols['借券賣出不含賣出總異動股數'], cols['借券賣出餘額股數'], cols['借券賣出次日限額股數']
        ]

        for name in stock_info_cols:
            df[name] = df[name].str.replace(' ','').replace('*','')

        for name in volume_cols:
            df[name] = df[name].str.replace(',','').astype(int)

        return df

version_0 = VERSION_0(
    schema.tables.StockDaily,
    {
        '股票代號': schema.tables.StockDaily.f_stock_info.代號,
        '融券賣出': schema.tables.StockDaily.f_short.融券賣出股數,
        '融券買進': schema.tables.StockDaily.f_short.融券買進股數,
        '融券現券': schema.tables.StockDaily.f_short.融券現償股數,
        '融券今日餘額': schema.tables.StockDaily.f_short.融券餘額股數,
        '融券次一營業日限額': schema.tables.StockDaily.f_short_additional.融券次日限額股數,
        '借券賣出賣出': schema.tables.StockDaily.f_short.借券賣出賣出股數,
        '借券賣出庫存異動': schema.tables.StockDaily.f_short.借券賣出不含賣出總異動股數,
        '借券賣出今日餘額': schema.tables.StockDaily.f_short.借券賣出餘額股數,
        '借券賣出可使用額度': schema.tables.StockDaily.f_short_additional.借券賣出次日限額股數
    },
    True
)

class VERSION_1(VERSION_0):
    
    def to_df(self, content):
        head_cols = []
        i = 0
        for group in content['groups']:
            span = group['span']
            title = group['title']
            head_cols.extend([title+n for n in content['fields'][i:i+span]])
            i += span
        assert len(head_cols) == 15
        df = pd.DataFrame(columns=head_cols, data=content['data'])
        df = df.loc[df['股票代號']!='']
        return df
        
    def format_dtype(self, df):
        cols = self.table.cols
        
        stock_info_cols= [cols['代號']]
        volume_cols = [
            cols['融券賣出股數'], cols['融券買進股數'], cols['融券現償股數'], cols['融券餘額股數'], cols['融券次日限額股數'],
            cols['借券賣出賣出股數'], cols['借券賣出還券股數'], cols['借券賣出調整股數'], cols['借券賣出餘額股數'], cols['借券賣出次日限額股數']
        ]

        for name in stock_info_cols:
            df[name] = df[name].str.replace(' ','').replace('*','')

        for name in volume_cols:
            df[name] = df[name].str.replace(',','').astype(int)

        return df
    
    def add_other_columns(self, df):
        cols = self.table.cols
        df[cols['借券賣出不含賣出總異動股數']] = df[cols['借券賣出調整股數']] - df[cols['借券賣出還券股數']]
        return df
    
version_1 = VERSION_1(
    schema.tables.StockDaily,
    {
        '股票代號': schema.tables.StockDaily.f_stock_info.代號,
        '融券賣出': schema.tables.StockDaily.f_short.融券賣出股數,
        '融券買進': schema.tables.StockDaily.f_short.融券買進股數,
        '融券現券': schema.tables.StockDaily.f_short.融券現償股數,
        '融券今日餘額': schema.tables.StockDaily.f_short.融券餘額股數,
        '融券次一營業日限額': schema.tables.StockDaily.f_short_additional.融券次日限額股數,
        '借券賣出當日賣出': schema.tables.StockDaily.f_short.借券賣出賣出股數,
        '借券賣出當日還券': schema.tables.StockDaily.f_short.借券賣出還券股數,
        '借券賣出當日調整': schema.tables.StockDaily.f_short.借券賣出調整股數,
        '借券賣出當日餘額': schema.tables.StockDaily.f_short.借券賣出餘額股數,
        '借券賣出次一營業日可限額': schema.tables.StockDaily.f_short_additional.借券賣出次日限額股數
    },
    True
)