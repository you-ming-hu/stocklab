from ..base import Source
from ... import schema

import json
import pandas as pd

class STOCKS_STAGE_1(Source):
    
    def open(self, file):
        with open(file, encoding="utf-8") as f:
            content = json.load(f)
        return content
    
    def check_empty(self, content):
        return content['stat'] == '很抱歉，沒有符合條件的資料!'
    
    def to_df(self, content):
        df = pd.DataFrame(columns=content['fields'], data=content['data'])
        assert len(df.columns) == 12
        return df
        
    def format_dtype(self, df):
        cols = self.table.cols
        
        stock_info_cols= [cols['代號']]
        volume_cols = [
            cols['外陸資買進股數'], cols['外陸資賣出股數'], 
            cols['投信買進股數'], cols['投信賣出股數'],
            cols['自營商買進股數'], cols['自營商賣出股數']
        ]

        for name in stock_info_cols:
            df[name] = df[name].str.replace(' ','').replace('*','')

        for name in volume_cols:
            df[name] = df[name].str.replace(',','').astype(int)

        return df

stocks_stage_1 = STOCKS_STAGE_1(
    schema.tables.StockDaily,
    {
        '證券代號': schema.tables.StockDaily.f_stock_info.代號,
        '外資買進股數': schema.tables.StockDaily.f_institution_share_flow.外陸資買進股數,
        '外資賣出股數': schema.tables.StockDaily.f_institution_share_flow.外陸資賣出股數,
        '投信買進股數': schema.tables.StockDaily.f_institution_share_flow.投信買進股數,
        '投信賣出股數': schema.tables.StockDaily.f_institution_share_flow.投信賣出股數,
        '自營商買進股數': schema.tables.StockDaily.f_institution_share_flow.自營商買進股數,
        '自營商賣出股數': schema.tables.StockDaily.f_institution_share_flow.自營商賣出股數
    },
    True
)

class STOCKS_STAGE_2(STOCKS_STAGE_1):
    def to_df(self, content):
        df = pd.DataFrame(columns=content['fields'], data=content['data'])
        assert len(df.columns) == 16
        return df
    
    def format_dtype(self, df):
        cols = self.table.cols
        
        stock_info_cols= [cols['代號']]
        volume_cols = [
            cols['外陸資買進股數'], cols['外陸資賣出股數'], 
            cols['投信買進股數'], cols['投信賣出股數'],
            cols['自營商_自行買賣_買進股數'], cols['自營商_自行買賣_賣出股數'],
            cols['自營商_避險_買進股數'], cols['自營商_避險_賣出股數']
        ]

        for name in stock_info_cols:
            df[name] = df[name].str.replace(' ','').replace('*','')

        for name in volume_cols:
            df[name] = df[name].str.replace(',','').astype(int)

        return df

    
    def add_other_columns(self, df):
        cols = self.table.cols
        df[cols['自營商買進股數']] = df[cols['自營商_自行買賣_買進股數']] + df[cols['自營商_避險_買進股數']]
        df[cols['自營商賣出股數']] = df[cols['自營商_自行買賣_賣出股數']] + df[cols['自營商_避險_賣出股數']]
        return df

stocks_stage_2 = STOCKS_STAGE_2(
    schema.tables.StockDaily,
    {
        '證券代號': schema.tables.StockDaily.f_stock_info.代號,
        '外資買進股數': schema.tables.StockDaily.f_institution_share_flow.外陸資買進股數,
        '外資賣出股數': schema.tables.StockDaily.f_institution_share_flow.外陸資賣出股數,
        '投信買進股數': schema.tables.StockDaily.f_institution_share_flow.投信買進股數,
        '投信賣出股數': schema.tables.StockDaily.f_institution_share_flow.投信賣出股數,
        '自營商買進股數(自行買賣)': schema.tables.StockDaily.f_institution_share_flow.自營商_自行買賣_買進股數,
        '自營商賣出股數(自行買賣)': schema.tables.StockDaily.f_institution_share_flow.自營商_自行買賣_賣出股數,
        '自營商買進股數(避險)': schema.tables.StockDaily.f_institution_share_flow.自營商_避險_買進股數,
        '自營商賣出股數(避險)': schema.tables.StockDaily.f_institution_share_flow.自營商_避險_賣出股數,
        
    },
    True
)

class STOCKS_STAGE_3(STOCKS_STAGE_1):
    def to_df(self, content):
        df = pd.DataFrame(columns=content['fields'], data=content['data'])
        assert len(df.columns) == 19
        return df
    
    def format_dtype(self, df):
        cols = self.table.cols
        
        stock_info_cols= [cols['代號']]
        volume_cols = [
            cols['外陸資_不含外資自營商_買進股數'], cols['外陸資_不含外資自營商_賣出股數'], 
            cols['外資自營商買進股數'], cols['外資自營商賣出股數'], 
            cols['投信買進股數'], cols['投信賣出股數'],
            cols['自營商_自行買賣_買進股數'], cols['自營商_自行買賣_賣出股數'],
            cols['自營商_避險_買進股數'], cols['自營商_避險_賣出股數']
        ]

        for name in stock_info_cols:
            df[name] = df[name].str.replace(' ','').replace('*','')

        for name in volume_cols:
            df[name] = df[name].str.replace(',','').astype(int)

        return df
    
    def add_other_columns(self, df):
        cols = self.table.cols
        df[cols['外陸資買進股數']] = df[cols['外陸資_不含外資自營商_買進股數']] + df[cols['外資自營商買進股數']]
        df[cols['外陸資賣出股數']] = df[cols['外陸資_不含外資自營商_賣出股數']] + df[cols['外資自營商賣出股數']]
        df[cols['自營商買進股數']] = df[cols['自營商_自行買賣_買進股數']] + df[cols['自營商_避險_買進股數']]
        df[cols['自營商賣出股數']] = df[cols['自營商_自行買賣_賣出股數']] + df[cols['自營商_避險_賣出股數']]
        return df

stocks_stage_3 = STOCKS_STAGE_3(
    schema.tables.StockDaily,
    {   
        '證券代號': schema.tables.StockDaily.f_stock_info.代號,
        '外陸資買進股數(不含外資自營商)': schema.tables.StockDaily.f_institution_share_flow.外陸資_不含外資自營商_買進股數,
        '外陸資賣出股數(不含外資自營商)': schema.tables.StockDaily.f_institution_share_flow.外陸資_不含外資自營商_賣出股數,
        '外資自營商買進股數': schema.tables.StockDaily.f_institution_share_flow.外資自營商買進股數,
        '外資自營商賣出股數': schema.tables.StockDaily.f_institution_share_flow.外資自營商賣出股數,
        '投信買進股數': schema.tables.StockDaily.f_institution_share_flow.投信買進股數,
        '投信賣出股數': schema.tables.StockDaily.f_institution_share_flow.投信賣出股數,
        '自營商買進股數(自行買賣)': schema.tables.StockDaily.f_institution_share_flow.自營商_自行買賣_買進股數,
        '自營商賣出股數(自行買賣)': schema.tables.StockDaily.f_institution_share_flow.自營商_自行買賣_賣出股數,
        '自營商買進股數(避險)': schema.tables.StockDaily.f_institution_share_flow.自營商_避險_買進股數,
        '自營商賣出股數(避險)': schema.tables.StockDaily.f_institution_share_flow.自營商_避險_賣出股數,
    },
    True
)


# class MARKET(STOCKS):
#     def to_df(self, content):
#         for table in content['tables']:
#             if 'title' in table:
#                 if '信用交易統計' in table['title']:
#                     break
#         df = pd.DataFrame(columns=table['fields'], data=table['data']).set_index('項目')
#         rearrange = {}
#         for item, values in df.iterrows():
#             for cate, value in values.items():
#                 rearrange[item+cate] = value
#         assert len(rearrange) == 15
#         df = pd.Series(rearrange).to_frame().T
#         return df
    
#     def format_dtype(self, df):
#         for name in df.columns:
#             df[name] = df[name].str.replace(',','').astype(int) * 1000
#         return df
    
    
# market = MARKET(
#     schema.tables.TWSEDaily,
#     {
#         '融資(交易單位)買進': schema.tables.TWSEDaily.f_margin.融資買進股數,
#         '融資(交易單位)賣出': schema.tables.TWSEDaily.f_margin.融資賣出股數,
#         '融資(交易單位)現金(券)償還': schema.tables.TWSEDaily.f_margin.融資現償股數,
#         '融資(交易單位)今日餘額': schema.tables.TWSEDaily.f_margin.融資餘額股數,
#         '融資金額(仟元)買進': schema.tables.TWSEDaily.f_margin_additional.融資買進金額,
#         '融資金額(仟元)賣出': schema.tables.TWSEDaily.f_margin_additional.融資賣出金額,
#         '融資金額(仟元)現金(券)償還': schema.tables.TWSEDaily.f_margin_additional.融資現償金額,
#         '融資金額(仟元)今日餘額': schema.tables.TWSEDaily.f_margin_additional.融資餘額金額
#     },
#     True
# )
