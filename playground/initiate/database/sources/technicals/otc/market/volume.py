from ....base import Source
from ..... import schema

import pandas as pd

class VERSION_0(Source):
    
    def check_empty(self, content):
        return content['stat'] != 'ok'
    
    def to_df(self, content):
        content = content['tables'][0]
        df = pd.DataFrame(
            data=content['data'],
            columns=content['fields'],
        )
        return df
        
    def format_dtype(self, df):
        date_col = schema.tables.OTCDaily.f_datatimestamp.資料日期
        ymd = df[date_col].str.split('/', expand=True).astype(int)
        ymd[0] = ymd[0] + 1911
        ymd = ymd.apply(lambda cols: pd.Timestamp(f'{cols[0]}{cols[1]:0>2}{cols[2]:0>2}'),axis=1)
        df[date_col] = ymd
        
        volume_cols = [
            schema.tables.OTCDaily.f_technicals_volume.交易股數,
            schema.tables.OTCDaily.f_technicals_volume.交易筆數,
            schema.tables.OTCDaily.f_technicals_volume.交易金額
        ]

        price_cols = [
            schema.tables.OTCDaily.f_technicals_price.收盤價
        ]

        for name in volume_cols:
            df[name] = df[name].str.replace(',','').astype(int)
            if name != schema.tables.OTCDaily.f_technicals_volume.交易筆數:
                df[name] = df[name]*1000

        for name in price_cols:
            df[name] = df[name].astype(float)

        return df

version_0 = VERSION_0(
    schema.tables.OTCDaily,
    {
        '日期': schema.tables.OTCDaily.f_datatimestamp.資料日期,
        '櫃買指數': schema.tables.OTCDaily.f_technicals_price.收盤價,
        '成交股數（仟股）': schema.tables.OTCDaily.f_technicals_volume.交易股數,
        '金額（仟元）': schema.tables.OTCDaily.f_technicals_volume.交易金額,
        '筆數': schema.tables.OTCDaily.f_technicals_volume.交易筆數
    },
    False
)

class VERSION_1(VERSION_0):
    pass

version_1 = VERSION_1(
    schema.tables.OTCDaily,
    {
        '日期': schema.tables.OTCDaily.f_datatimestamp.資料日期,
        '櫃買指數': schema.tables.OTCDaily.f_technicals_price.收盤價,
        '成交張數': schema.tables.OTCDaily.f_technicals_volume.交易股數,
        '金額（仟元）': schema.tables.OTCDaily.f_technicals_volume.交易金額,
        '筆數': schema.tables.OTCDaily.f_technicals_volume.交易筆數
    },
    False
)
