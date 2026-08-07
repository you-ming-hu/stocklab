from ....base import Source
from ..... import schema

import pandas as pd

class VERSION_0(Source):
    
    def check_empty(self, content):
        return content['stat'] != 'OK'
    
    def to_df(self, content):
        df = pd.DataFrame(
            data=content['data'],
            columns=content['fields'],
        )
        return df
        
    def format_dtype(self, df):
        taiwan_date_cols = [
            schema.tables.TWSEDaily.f_datatimestamp.資料日期
        ]
        float_cols = [
            schema.tables.TWSEDaily.f_technicals_price.開盤價,
            schema.tables.TWSEDaily.f_technicals_price.最高價,
            schema.tables.TWSEDaily.f_technicals_price.最低價,
            schema.tables.TWSEDaily.f_technicals_price.收盤價
        ]
        df = super().format_dtype(df, float_cols=float_cols, taiwan_date_cols=taiwan_date_cols)
        return df

version_0 = VERSION_0(
    schema.tables.TWSEDaily,
    {
        '日期': schema.tables.TWSEDaily.f_datatimestamp.資料日期,
        '開盤指數': schema.tables.TWSEDaily.f_technicals_price.開盤價,
        '最高指數': schema.tables.TWSEDaily.f_technicals_price.最高價,
        '最低指數': schema.tables.TWSEDaily.f_technicals_price.最低價,
        '收盤指數': schema.tables.TWSEDaily.f_technicals_price.收盤價
    },
    False
)