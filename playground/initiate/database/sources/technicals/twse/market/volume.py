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
        date_col = '日期'
        ymd = df[date_col].str.split('/', expand=True).astype(int)
        ymd[0] = ymd[0] + 1911
        ymd = ymd.apply(lambda cols: pd.Timestamp(f'{cols[0]}{cols[1]:0>2}{cols[2]:0>2}'),axis=1)
        df[date_col] = ymd
        
        for name in ['成交股數','成交金額','成交筆數']:
            df[name] = df[name].str.replace(',','').astype(int)

        for name in ['發行量加權股價指數']:
            df[name] = df[name].str.replace(',','').astype(float)

        return df

version_0 = VERSION_0(
    schema.tables.TWSEDaily,
    {
        '日期': schema.tables.TWSEDaily.f_datatimestamp.資料日期,
        '發行量加權股價指數': schema.tables.TWSEDaily.f_technicals_price.收盤價,
        '成交股數': schema.tables.TWSEDaily.f_technicals_volume.交易股數,
        '成交金額': schema.tables.TWSEDaily.f_technicals_volume.交易金額,
        '成交筆數': schema.tables.TWSEDaily.f_technicals_volume.交易筆數
    },
    False
)
