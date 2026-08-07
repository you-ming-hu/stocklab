from ...base import Source

import pandas as pd

class MARGIN(Source):
    
    def check_empty(self, content):
        return content['stat'] == '很抱歉，沒有符合條件的資料'
    
class SHORT_SBL_VOLUME(Source):
    
    def check_empty(self, content):
        return content['data'] == []
    
    def to_df(self, content, column_count):
        head_cols = []
        i = 0
        for group in content['groups']:
            span = group['span']
            title = group['title']
            head_cols.extend([title+n for n in content['fields'][i:i+span]])
            i += span
        assert len(head_cols) == column_count
        df = pd.DataFrame(columns=head_cols, data=content['data'])
        df = df.loc[df['股票代號']!='']
        return df
    
class SHORT_SBL_VALUE(Source):
    
    def check_empty(self, content):
        return content['stat'] == '很抱歉，沒有符合條件的資料!'

class SUM(Source):
    def format_dtype(self, df):
        df = super().format_dtype(df, int_cols=df.columns)
        df = df.sum(axis=0).to_frame().T
        return df