from ...base import Source

import json
import pandas as pd

class MARGIN(Source):
    def open(self, file):
        with open(file, encoding="utf-8") as f:
            content = json.load(f)
        return content
    
    def check_empty(self, content):
        return content['stat'] == '很抱歉，沒有符合條件的資料'
    
class SHORT_SBL(Source):
    def open(self, file):
        with open(file, encoding="utf-8") as f:
            content = json.load(f)
        return content
    
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
        
    def format_dtype(self, df, stock_info_cols, volume_cols):
        for name in stock_info_cols:
            df[name] = df[name].str.replace(' ','').replace('*','')
        for name in volume_cols:
            df[name] = df[name].str.replace(',','').astype(int)
        return df
    
class SUM:
    def format_dtype(self, df):
        for name in df.columns:
            df[name] = df[name].str.replace(',','').astype(int)
        df = df.sum(axis=0).to_frame().T
        return df