from ...base import Source

import pandas as pd
import pathlib
from bs4 import BeautifulSoup

class FLOW_VOLUME(Source):
    
    def check_empty(self, content, name='totalCount'):
        target_table = None
        for table in content['tables']:
            if name in table:
                target_table = table
        return len(target_table['data']) == 0
    
    def to_df(self, content, name='totalCount'):
        target_table = None
        for table in content['tables']:
            if name in table:
                target_table = table
        assert target_table is not None
        df = pd.DataFrame(
            target_table['data'],
            columns=target_table['fields'],
        )
        return df
    
class BALANCE_VOLUME(Source):
    def open(self, file):
        content = pathlib.Path(file).read_text('utf-8')
        return content
    
    def check_empty(self, content):
        return '查無所需資料' in content
    
    def to_df(self, content, column_count):
        content = BeautifulSoup(content, "html.parser")
        table = content.find_all('table')[0]
        rows = table.find_all('tr')

        col_idx = 1
        columns = [c.text for c in rows[col_idx].find_all('th')]
        data = [[c.text for c in r.find_all('td')] for r in rows[col_idx+1:]]

        df = pd.DataFrame(data, columns=columns)
        assert len(df.columns) == column_count, len(df.columns)
        return df

class FLOW_VALUE(Source):
    def check_empty(self, content):
        return len(content['tables'][0]['data']) == 0

    def to_df(self, content):
        table = content['tables'][0]
        df = pd.DataFrame(
            [r for r in table['data'] if len(r)!=0],
            columns=table['fields'],
        )
        return df

    def flat_df(self, df, index_name, column_count):
        df = df.set_index(index_name)
        rearrange = {}
        for item, values in df.iterrows():
            for cate, value in values.items():
                rearrange[item.replace('\u3000','')+cate] = value
        assert len(rearrange) == column_count, len(rearrange)
        df = pd.Series(rearrange).to_frame().T
        return df

class SUM(Source):
    def format_dtype(self, df):
        df = super().format_dtype(df, int_cols=df.columns)
        df = df.sum(axis=0).to_frame().T
        return df