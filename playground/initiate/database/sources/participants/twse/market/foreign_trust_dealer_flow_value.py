from ..... import schema

from ..base import FLOW_VALUE

import pandas as pd

class VERSION_0(FLOW_VALUE):
    def to_df(self, content):
        df = super().to_df(content, 12)
        return df

version_0 = VERSION_0(
    schema.tables.TWSEDaily,
    {   
        '外資買進金額': schema.tables.TWSEDaily.f_foreign_flow_value.外陸資_買進_金額,
        '外資賣出金額': schema.tables.TWSEDaily.f_foreign_flow_value.外陸資_賣出_金額,
        '投信買進金額': schema.tables.TWSEDaily.f_trust_flow_value.投信_買進_金額,
        '投信賣出金額': schema.tables.TWSEDaily.f_trust_flow_value.投信_賣出_金額,
        '自營商買進金額': schema.tables.TWSEDaily.f_dealer_flow_value.自營商_買進_金額,
        '自營商賣出金額': schema.tables.TWSEDaily.f_dealer_flow_value.自營商_賣出_金額,
    },
    True
)

class VERSION_1(FLOW_VALUE):
    def to_df(self, content):
        df = super().to_df(content, 12)
        return df

version_1 = VERSION_1(
    schema.tables.TWSEDaily,
    {   
        '外資及陸資買進金額': schema.tables.TWSEDaily.f_foreign_flow_value.外陸資_買進_金額,
        '外資及陸資賣出金額': schema.tables.TWSEDaily.f_foreign_flow_value.外陸資_賣出_金額,
        '投信買進金額': schema.tables.TWSEDaily.f_trust_flow_value.投信_買進_金額,
        '投信賣出金額': schema.tables.TWSEDaily.f_trust_flow_value.投信_賣出_金額,
        '自營商買進金額': schema.tables.TWSEDaily.f_dealer_flow_value.自營商_買進_金額,
        '自營商賣出金額': schema.tables.TWSEDaily.f_dealer_flow_value.自營商_賣出_金額,
    },
    True
)

class VERSION_2(FLOW_VALUE):
    def to_df(self, content):
        df = super().to_df(content, 15)
        return df
    
    def add_other_columns(self, df):
        cols = self.table.columns
        df[cols['自營商_買進_金額']] = df[cols['自營商_自行買賣_買進_金額']] + df[cols['自營商_避險_買進_金額']]
        df[cols['自營商_賣出_金額']] = df[cols['自營商_自行買賣_賣出_金額']] + df[cols['自營商_避險_賣出_金額']]
        return df

version_2 = VERSION_2(
    schema.tables.TWSEDaily,
    {   
        '外資及陸資買進金額': schema.tables.TWSEDaily.f_foreign_flow_value.外陸資_買進_金額,
        '外資及陸資賣出金額': schema.tables.TWSEDaily.f_foreign_flow_value.外陸資_賣出_金額,
        '投信買進金額': schema.tables.TWSEDaily.f_trust_flow_value.投信_買進_金額,
        '投信賣出金額': schema.tables.TWSEDaily.f_trust_flow_value.投信_賣出_金額,
        '自營商(自行買賣)買進金額': schema.tables.TWSEDaily.f_dealer_flow_value.自營商_自行買賣_買進_金額,
        '自營商(自行買賣)賣出金額': schema.tables.TWSEDaily.f_dealer_flow_value.自營商_自行買賣_賣出_金額,
        '自營商(避險)買進金額': schema.tables.TWSEDaily.f_dealer_flow_value.自營商_避險_買進_金額,
        '自營商(避險)賣出金額': schema.tables.TWSEDaily.f_dealer_flow_value.自營商_避險_賣出_金額,
    },
    True
)

class VERSION_3(FLOW_VALUE):
    def to_df(self, content):
        df = super().to_df(content, 18)
        return df
    
    def add_other_columns(self, df):
        cols = self.table.columns
        df[cols['自營商_買進_金額']] = df[cols['自營商_自行買賣_買進_金額']] + df[cols['自營商_避險_買進_金額']]
        df[cols['自營商_賣出_金額']] = df[cols['自營商_自行買賣_賣出_金額']] + df[cols['自營商_避險_賣出_金額']]
        df[cols['外陸資_買進_金額']] = df[cols['外陸資_不含外資自營商_買進_金額']] + df[cols['外資自營商_買進_金額']]
        df[cols['外陸資_賣出_金額']] = df[cols['外陸資_不含外資自營商_賣出_金額']] + df[cols['外資自營商_賣出_金額']]
        return df

version_3 = VERSION_3(
    schema.tables.TWSEDaily,
    {   
        '外資及陸資(不含外資自營商)買進金額': schema.tables.TWSEDaily.f_foreign_flow_value.外陸資_不含外資自營商_買進_金額,
        '外資及陸資(不含外資自營商)賣出金額': schema.tables.TWSEDaily.f_foreign_flow_value.外陸資_不含外資自營商_賣出_金額,
        '外資自營商買進金額': schema.tables.TWSEDaily.f_foreign_flow_value.外資自營商_買進_金額,
        '外資自營商賣出金額': schema.tables.TWSEDaily.f_foreign_flow_value.外資自營商_賣出_金額,
        '投信買進金額': schema.tables.TWSEDaily.f_trust_flow_value.投信_買進_金額,
        '投信賣出金額': schema.tables.TWSEDaily.f_trust_flow_value.投信_賣出_金額,
        '自營商(自行買賣)買進金額': schema.tables.TWSEDaily.f_dealer_flow_value.自營商_自行買賣_買進_金額,
        '自營商(自行買賣)賣出金額': schema.tables.TWSEDaily.f_dealer_flow_value.自營商_自行買賣_賣出_金額,
        '自營商(避險)買進金額': schema.tables.TWSEDaily.f_dealer_flow_value.自營商_避險_買進_金額,
        '自營商(避險)賣出金額': schema.tables.TWSEDaily.f_dealer_flow_value.自營商_避險_賣出_金額,
    },
    True
)
