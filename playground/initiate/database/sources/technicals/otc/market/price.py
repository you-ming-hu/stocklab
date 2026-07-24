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
        
        df[date_col] = df[date_col].apply(pd.Timestamp)

        price_cols = [
            schema.tables.OTCDaily.f_technicals_price.開盤價,
            schema.tables.OTCDaily.f_technicals_price.最高價,
            schema.tables.OTCDaily.f_technicals_price.最低價,
            schema.tables.OTCDaily.f_technicals_price.收盤價
        ]

        for name in price_cols:
            df[name] = df[name].str.replace(',','').astype(float)
        return df

version_0 = VERSION_0(
    schema.tables.OTCDaily,
    {
        '日期': schema.tables.OTCDaily.f_datatimestamp.資料日期,
        '開市': schema.tables.OTCDaily.f_technicals_price.開盤價,
        '最高': schema.tables.OTCDaily.f_technicals_price.最高價,
        '最低': schema.tables.OTCDaily.f_technicals_price.最低價,
        '收市': schema.tables.OTCDaily.f_technicals_price.收盤價
    },
    False
)