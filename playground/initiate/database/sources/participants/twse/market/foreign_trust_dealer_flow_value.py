from ..... import schema
from ..stocks import foreign_trust_dealer_volume as base

import pandas as pd

class VERSION_0(base.VERSION_0):
    def to_df(self, content):
        df = pd.DataFrame(columns=content['fields'], data=content['data']).set_index('單位名稱')
        rearrange = {}
        for item, values in df.iterrows():
            for cate, value in values.items():
                rearrange[item+cate] = value
        assert len(rearrange) == 12
        df = pd.Series(rearrange).to_frame().T
        return df

    def format_dtype(self, df):
        for name in df.columns:
            df[name] = df[name].str.replace(',','').astype(int)
        return df

version_0 = VERSION_0(
    schema.tables.TWSEDaily,
    {   
        '自營商買進金額': schema.tables.TWSEDaily.f_institution_fund_flow.自營商買進金額,
        '自營商賣出金額': schema.tables.TWSEDaily.f_institution_fund_flow.自營商賣出金額,
        '投信買進金額': schema.tables.TWSEDaily.f_institution_fund_flow.投信買進金額,
        '投信賣出金額': schema.tables.TWSEDaily.f_institution_fund_flow.投信賣出金額,
        '外資買進金額': schema.tables.TWSEDaily.f_institution_fund_flow.外陸資買進金額,
        '外資賣出金額': schema.tables.TWSEDaily.f_institution_fund_flow.外陸資賣出金額,
    },
    True
)

class VERSION_1(VERSION_0):
    pass

version_1 = VERSION_1(
    schema.tables.TWSEDaily,
    {   
        '自營商買進金額': schema.tables.TWSEDaily.f_institution_fund_flow.自營商買進金額,
        '自營商賣出金額': schema.tables.TWSEDaily.f_institution_fund_flow.自營商賣出金額,
        '投信買進金額': schema.tables.TWSEDaily.f_institution_fund_flow.投信買進金額,
        '投信賣出金額': schema.tables.TWSEDaily.f_institution_fund_flow.投信賣出金額,
        '外資及陸資買進金額': schema.tables.TWSEDaily.f_institution_fund_flow.外陸資買進金額,
        '外資及陸資賣出金額': schema.tables.TWSEDaily.f_institution_fund_flow.外陸資賣出金額,
    },
    True
)

class VERSION_2(VERSION_0):
    def to_df(self, content):
        df = pd.DataFrame(columns=content['fields'], data=content['data']).set_index('單位名稱')
        rearrange = {}
        for item, values in df.iterrows():
            for cate, value in values.items():
                rearrange[item+cate] = value
        assert len(rearrange) == 15
        df = pd.Series(rearrange).to_frame().T
        return df
    
    def add_other_columns(self, df):
        cols = self.table.cols
        df[cols['自營商買進金額']] = df[cols['自營商_自行買賣_買進金額']] + df[cols['自營商_避險_買進金額']]
        df[cols['自營商賣出金額']] = df[cols['自營商_自行買賣_賣出金額']] + df[cols['自營商_避險_賣出金額']]
        return df

version_2 = VERSION_2(
    schema.tables.TWSEDaily,
    {   
        '自營商(自行買賣)買進金額': schema.tables.TWSEDaily.f_institution_fund_flow.自營商_自行買賣_買進金額,
        '自營商(自行買賣)賣出金額': schema.tables.TWSEDaily.f_institution_fund_flow.自營商_自行買賣_賣出金額,
        '自營商(避險)買進金額': schema.tables.TWSEDaily.f_institution_fund_flow.自營商_避險_買進金額,
        '自營商(避險)賣出金額': schema.tables.TWSEDaily.f_institution_fund_flow.自營商_避險_賣出金額,
        '投信買進金額': schema.tables.TWSEDaily.f_institution_fund_flow.投信買進金額,
        '投信賣出金額': schema.tables.TWSEDaily.f_institution_fund_flow.投信賣出金額,
        '外資及陸資買進金額': schema.tables.TWSEDaily.f_institution_fund_flow.外陸資買進金額,
        '外資及陸資賣出金額': schema.tables.TWSEDaily.f_institution_fund_flow.外陸資賣出金額,
    },
    True
)

class VERSION_3(VERSION_0):
    def to_df(self, content):
        df = pd.DataFrame(columns=content['fields'], data=content['data']).set_index('單位名稱')
        rearrange = {}
        for item, values in df.iterrows():
            for cate, value in values.items():
                rearrange[item+cate] = value
        assert len(rearrange) == 18
        df = pd.Series(rearrange).to_frame().T
        return df
    
    def add_other_columns(self, df):
        cols = self.table.cols
        df[cols['自營商買進金額']] = df[cols['自營商_自行買賣_買進金額']] + df[cols['自營商_避險_買進金額']]
        df[cols['自營商賣出金額']] = df[cols['自營商_自行買賣_賣出金額']] + df[cols['自營商_避險_賣出金額']]
        df[cols['外陸資買進金額']] = df[cols['外陸資_不含外資自營商_買進金額']] + df[cols['外資自營商買進金額']]
        df[cols['外陸資賣出金額']] = df[cols['外陸資_不含外資自營商_賣出金額']] + df[cols['外資自營商賣出金額']]
        return df

version_3 = VERSION_3(
    schema.tables.TWSEDaily,
    {   
        '自營商(自行買賣)買進金額': schema.tables.TWSEDaily.f_institution_fund_flow.自營商_自行買賣_買進金額,
        '自營商(自行買賣)賣出金額': schema.tables.TWSEDaily.f_institution_fund_flow.自營商_自行買賣_賣出金額,
        '自營商(避險)買進金額': schema.tables.TWSEDaily.f_institution_fund_flow.自營商_避險_買進金額,
        '自營商(避險)賣出金額': schema.tables.TWSEDaily.f_institution_fund_flow.自營商_避險_賣出金額,
        '投信買進金額': schema.tables.TWSEDaily.f_institution_fund_flow.投信買進金額,
        '投信賣出金額': schema.tables.TWSEDaily.f_institution_fund_flow.投信賣出金額,
        '外資及陸資(不含外資自營商)買進金額': schema.tables.TWSEDaily.f_institution_fund_flow.外陸資_不含外資自營商_買進金額,
        '外資及陸資(不含外資自營商)賣出金額': schema.tables.TWSEDaily.f_institution_fund_flow.外陸資_不含外資自營商_賣出金額,
        '外資自營商買進金額': schema.tables.TWSEDaily.f_institution_fund_flow.外資自營商買進金額,
        '外資自營商賣出金額': schema.tables.TWSEDaily.f_institution_fund_flow.外資自營商賣出金額,
    },
    True
)
