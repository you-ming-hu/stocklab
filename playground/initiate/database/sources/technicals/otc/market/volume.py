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
        taiwan_date_cols = [
            schema.tables.OTCDaily.f_datatimestamp.資料日期
        ]
        int_cols = [
            schema.tables.OTCDaily.f_technicals_volume.交易股數,
            schema.tables.OTCDaily.f_technicals_volume.交易金額,
            schema.tables.OTCDaily.f_technicals_volume.交易筆數,
        ]
        float_cols = [
            schema.tables.OTCDaily.f_technicals_price.收盤價
        ]
        df = super().format_dtype(df, int_cols=int_cols, float_cols=float_cols, taiwan_date_cols=taiwan_date_cols)

        thousand_cols = int_cols[:2]
        df[thousand_cols] = df[thousand_cols] * 1000
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
