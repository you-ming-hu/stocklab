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
        return content['stat'] == '很抱歉，沒有符合條件的資料'
    
    def to_df(self, content):
        for table in content['tables']:
            if 'title' in table:
                if '融資融券彙總' in table['title']:
                    break

        head_cols = []
        i = 0
        for group in table['groups']:
            span = group['span']
            title = group['title']
            head_cols.extend([title+n for n in table['fields'][i:i+span]])
            i += span

        assert len(head_cols) == 16
        df = pd.DataFrame(columns=head_cols, data=table['data'])
        return df
        
    def format_dtype(self, df):
        cols = self.table.cols
        
        stock_info_cols= [cols['代號']]
        volume_cols = [
            cols['融資買進股數'], cols['融資賣出股數'], cols['融資現償股數'], cols['融資餘額股數']
        ]

        for name in stock_info_cols:
            df[name] = df[name].str.replace(' ','').replace('*','')

        for name in volume_cols:
            df[name] = df[name].str.replace(',','').astype(int) * 1000

        return df

version_0 = VERSION_0(
    schema.tables.StockDaily,
    {
        '股票代號': schema.tables.StockDaily.f_stock_info.代號,
        '融資買進': schema.tables.StockDaily.f_margin.融資買進股數,
        '融資賣出': schema.tables.StockDaily.f_margin.融資賣出股數,
        '融資現金償還': schema.tables.StockDaily.f_margin.融資現償股數,
        '融資今日餘額': schema.tables.StockDaily.f_margin.融資餘額股數,
    },
    True
)