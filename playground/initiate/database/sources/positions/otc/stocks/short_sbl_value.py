# from ..... import schema

# from ..base import SHORT_SBL_VALUE

# import pandas as pd

# class VERSION_0(SHORT_SBL_VALUE):
    
#     def to_df(self, content):
#         head_cols = []
#         i = 0
#         for group in content['groups']:
#             span = group['span']
#             title = group['title']
#             head_cols.extend([title+n for n in content['fields'][i:i+span]])
#             i += span

#         assert len(head_cols) == 5
#         df = pd.DataFrame(columns=head_cols, data=content['data'])
#         df = df.loc[df['證券名稱']!='合計']
#         df['證券名稱'] = df['證券名稱'].str.split(' ',n=1,expand=True)[0]
#         return df
        
#     def format_dtype(self, df):
#         stock_info_cols= [
#             schema.tables.StockDaily.f_stock_info.代號,
#         ]
#         volume_cols = [
#             schema.tables.StockDaily.f_short_flow_value.融券_賣出_金額,
#             schema.tables.StockDaily.f_sbl_flow_value.借券賣出_賣出_金額
#         ]
#         for name in stock_info_cols:
#             df[name] = df[name].str.replace(' ','').replace('*','')
#         for name in volume_cols:
#             df[name] = df[name].str.replace(',','').astype(int)
#         return df

# version_0 = VERSION_0(
#     schema.tables.StockDaily,
#     {   
#         '證券名稱': schema.tables.StockDaily.f_stock_info.代號, 
#         '融券賣出成交金額': schema.tables.StockDaily.f_short_flow_value.融券_賣出_金額, 
#         '借券賣出成交金額': schema.tables.StockDaily.f_sbl_flow_value.借券賣出_賣出_金額
#     },
#     True
# )
