from ....base import Source
from ..... import schema

import json
import pandas as pd

class VERSION_0(Source):
    
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

version_0 = VERSION_0(
    schema.tables.StockDaily,
    {
        '證券代號': schema.tables.StockDaily.f_stock_info.代號,
        '外資買進股數': schema.tables.StockDaily.f_foreign_flow_volume.外陸資_買進_股數,
        '外資賣出股數': schema.tables.StockDaily.f_foreign_flow_volume.外陸資_賣出_股數,
        '投信買進股數': schema.tables.StockDaily.f_trust_flow_volume.投信_買進_股數,
        '投信賣出股數': schema.tables.StockDaily.f_trust_flow_volume.投信_賣出_股數,
        '自營商買進股數': schema.tables.StockDaily.f_dealer_flow_volume.自營商_買進_股數,
        '自營商賣出股數': schema.tables.StockDaily.f_dealer_flow_volume.自營商_賣出_股數
    },
    True
)

class VERSION_1(VERSION_0):
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

version_1 = VERSION_1(
    schema.tables.StockDaily,
    {
        '證券代號': schema.tables.StockDaily.f_stock_info.代號,
        '外資買進股數': schema.tables.StockDaily.f_foreign_flow_volume.外陸資_買進_股數,
        '外資賣出股數': schema.tables.StockDaily.f_foreign_flow_volume.外陸資_賣出_股數,
        '投信買進股數': schema.tables.StockDaily.f_trust_flow_volume.投信_買進_股數,
        '投信賣出股數': schema.tables.StockDaily.f_trust_flow_volume.投信_賣出_股數,
        '自營商買進股數(自行買賣)': schema.tables.StockDaily.f_dealer_flow_volume.自營商_自行買賣_買進_股數,
        '自營商賣出股數(自行買賣)': schema.tables.StockDaily.f_dealer_flow_volume.自營商_自行買賣_賣出_股數,
        '自營商買進股數(避險)': schema.tables.StockDaily.f_dealer_flow_volume.自營商_避險_買進_股數,
        '自營商賣出股數(避險)': schema.tables.StockDaily.f_dealer_flow_volume.自營商_避險_賣出_股數,
    },
    True
)

class VERSION_2(VERSION_0):
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

version_2 = VERSION_2(
    schema.tables.StockDaily,
    {   
        '證券代號': schema.tables.StockDaily.f_stock_info.代號,
        '外陸資買進股數(不含外資自營商)': schema.tables.StockDaily.f_foreign_flow_volume.外陸資_不含外資自營商_買進_股數,
        '外陸資賣出股數(不含外資自營商)': schema.tables.StockDaily.f_foreign_flow_volume.外陸資_不含外資自營商_賣出_股數,
        '外資自營商買進股數': schema.tables.StockDaily.f_foreign_flow_volume.外資自營商_買進_股數,
        '外資自營商賣出股數': schema.tables.StockDaily.f_foreign_flow_volume.外資自營商_賣出_股數,
        '投信買進股數': schema.tables.StockDaily.f_trust_flow_volume.投信_買進_股數,
        '投信賣出股數': schema.tables.StockDaily.f_trust_flow_volume.投信_賣出_股數,
        '自營商買進股數(自行買賣)': schema.tables.StockDaily.f_dealer_flow_volume.自營商_自行買賣_買進_股數,
        '自營商賣出股數(自行買賣)': schema.tables.StockDaily.f_dealer_flow_volume.自營商_自行買賣_賣出_股數,
        '自營商買進股數(避險)': schema.tables.StockDaily.f_dealer_flow_volume.自營商_避險_買進_股數,
        '自營商賣出股數(避險)': schema.tables.StockDaily.f_dealer_flow_volume.自營商_避險_賣出_股數,
    },
    True
)
