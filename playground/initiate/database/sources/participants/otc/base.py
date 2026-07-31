from ...base import Source

import pandas as pd

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
    
    def format_dtype(self, df, stock_info_cols, volume_cols):
        for name in stock_info_cols:
            df[name] = df[name].str.replace(' ','').replace('*','')
        for name in volume_cols:
            df[name] = df[name].str.replace(',','').astype(int)
        return df
    
# class BALANCE_VOLUME(Source):
    
#     def check_empty(self, content):
#         return content['data'] == []
    
#     def to_df(self, content, column_count):
#         df = pd.DataFrame(columns=content['fields'], data=content['data'])
#         assert len(df.columns) == column_count
#         return df
    
#     def format_dtype(self, df, stock_info_cols, volume_cols, ratio_cols):
#         for name in stock_info_cols:
#             df[name] = df[name].str.replace(' ','').replace('*','')
#         for name in volume_cols:
#             df[name] = df[name].str.replace(',','').astype(int)
#         for name in ratio_cols:
#             df[name] = df[name].astype(float)
#         return df

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

    def format_dtype(self, df):
        for name in df.columns:
            if df[name].dtype == int:
                continue
            df[name] = df[name].str.replace(',','').astype(int)
        return df

class SUM:
    def format_dtype(self, df):
        for name in df.columns:
            df[name] = df[name].str.replace(',','').astype(int)
        df = df.sum(axis=0).to_frame().T
        return df