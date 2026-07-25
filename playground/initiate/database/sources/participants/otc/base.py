from ...base import Source

import pandas as pd

class FLOW_VOLUME(Source):
    
    def check_empty(self, content, name='totalCount'):
        target_table = None
        for table in content['tables']:
            if name in table:
                target_table = table
        return len(target_table['data']) == 0
        # return len(content['tables'][0]['data']) == 0
    
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

# class FLOW_VALUE(Source):
    
#     def check_empty(self, content):
#         return content['stat'] == '很抱歉，沒有符合條件的資料!'

#     def to_df(self, content, column_count):
#         df = pd.DataFrame(columns=content['fields'], data=content['data']).set_index('單位名稱')
#         rearrange = {}
#         for item, values in df.iterrows():
#             for cate, value in values.items():
#                 rearrange[item+cate] = value
#         assert len(rearrange) == column_count
#         df = pd.Series(rearrange).to_frame().T
#         return df

#     def format_dtype(self, df):
#         for name in df.columns:
#             df[name] = df[name].str.replace(',','').astype(int)
#         return df

class SUM:
    def format_dtype(self, df):
        for name in df.columns:
            df[name] = df[name].str.replace(',','').astype(int)
        df = df.sum(axis=0).to_frame().T
        return df