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
        return content['stat'] != 'OK'
    
    def to_df(self, content):
        df = pd.DataFrame(
            data=content['data'],
            columns=content['fields'],
        )
        return df
        
    def format_dtype(self, df):
        cols = self.table.cols
        
        date_col = cols['資料日期']
        ymd = df[date_col].str.split('/', expand=True).astype(int)
        ymd[0] = ymd[0] + 1911
        ymd = ymd.apply(lambda cols: pd.Timestamp(f'{cols[0]}{cols[1]:0>2}{cols[2]:0>2}'),axis=1)
        df[date_col] = ymd

        for name in [cols['開盤價'], cols['最高價'], cols['最低價'], cols['收盤價']]:
            df[name] = df[name].str.replace(',','').astype(float)
        return df

version_0 = VERSION_0(
    schema.tables.TWSEDaily,
    {
        '日期': schema.tables.TWSEDaily.f_datatimestamp.資料日期,
        '開盤指數': schema.tables.TWSEDaily.f_techicals.開盤價,
        '最高指數': schema.tables.TWSEDaily.f_techicals.最高價,
        '最低指數': schema.tables.TWSEDaily.f_techicals.最低價,
        '收盤指數': schema.tables.TWSEDaily.f_techicals.收盤價
    },
    False
)