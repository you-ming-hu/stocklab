from ...base import Source

import pandas as pd

class FLOW_VOLUME(Source):
    
    def check_empty(self, content):
        return content['stat'] == '很抱歉，沒有符合條件的資料!'
    
    def to_df(self, content, column_count):
        df = pd.DataFrame(columns=content['fields'], data=content['data'])
        assert len(df.columns) == column_count
        return df
    
class BALANCE_VOLUME(Source):
    
    def check_empty(self, content):
        return content['data'] == []
    
    def to_df(self, content, column_count):
        df = pd.DataFrame(columns=content['fields'], data=content['data'])
        assert len(df.columns) == column_count
        return df

class FLOW_VALUE(Source):
    
    def check_empty(self, content):
        return content['stat'] == '很抱歉，沒有符合條件的資料!'

    def to_df(self, content, column_count):
        df = pd.DataFrame(columns=content['fields'], data=content['data']).set_index('單位名稱')
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

class SUM(Source):
    def format_dtype(self, df):
        df = super().format_dtype(df, int_cols=df.columns)
        df = df.sum(axis=0).to_frame().T
        return df