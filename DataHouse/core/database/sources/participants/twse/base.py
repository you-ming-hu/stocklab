from ...base import Source, SUM

import pandas as pd

class FLOW_VOLUME_V0(Source):
    
    def check_empty(self, content):
        return content['stat'] == '很抱歉，沒有符合條件的資料!'
    
    def to_df(self, content, column_count):
        df = super().to_df(content)
        assert len(df.columns) == column_count
        return df
    
class BALANCE_VOLUME_V0(Source):
    
    def check_empty(self, content):
        return content['data'] == []
    
    def to_df(self, content, column_count):
        df = super().to_df(content)
        assert len(df.columns) == column_count
        return df

class FLOW_VALUE_V0(Source):
    
    def check_empty(self, content):
        return content['stat'] == '很抱歉，沒有符合條件的資料!'

    def to_df(self, content, column_count):
        df = super().to_df(content)
        df = df.set_index('單位名稱')
        rearrange = {}
        for item, values in df.iterrows():
            for cate, value in values.items():
                rearrange[item+cate] = value
        assert len(rearrange) == column_count
        df = pd.Series(rearrange).to_frame().T
        return df

    def format_dtype(self, df):
        df = super().format_dtype(df, int_cols=df.columns)
        return df