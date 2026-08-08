from ..... import schema

from ..base import FLOW_VALUE

import pathlib
import pandas as pd

class VERSION_0(FLOW_VALUE):
    
    def open(self, file):
        return super().open(file, 'text', True)
    
    def check_empty(self, content):
        content, file = content
        return 'Sorry, the page you requested was not found.' in content
    
    def to_df(self, content):
        content, file = content
        df = pd.read_html(file)[0]
        df = super().flat_df(df,'單位名稱', 12)
        return df
        
version_0 = VERSION_0(
    schema.tables.OTCDaily,
    {   
        '外資買進金額(元)': schema.tables.OTCDaily.f_foreign_flow_value.外陸資_買進_金額,
        '外資賣出金額(元)': schema.tables.OTCDaily.f_foreign_flow_value.外陸資_賣出_金額,
        '投信買進金額(元)': schema.tables.OTCDaily.f_trust_flow_value.投信_買進_金額,
        '投信賣出金額(元)': schema.tables.OTCDaily.f_trust_flow_value.投信_賣出_金額,
        '自營商買進金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_買進_金額,
        '自營商賣出金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_賣出_金額,
    },
    True
)

class VERSION_1(FLOW_VALUE):

    def to_df(self, content):
        df = super().to_df(content)
        df = self.flat_df(df,'單位名稱', 12)
        return df

version_1 = VERSION_1(
    schema.tables.OTCDaily,
    {   
        '外資買進金額(元)': schema.tables.OTCDaily.f_foreign_flow_value.外陸資_買進_金額,
        '外資賣出金額(元)': schema.tables.OTCDaily.f_foreign_flow_value.外陸資_賣出_金額,
        '投信買進金額(元)': schema.tables.OTCDaily.f_trust_flow_value.投信_買進_金額,
        '投信賣出金額(元)': schema.tables.OTCDaily.f_trust_flow_value.投信_賣出_金額,
        '自營商買進金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_買進_金額,
        '自營商賣出金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_賣出_金額,
    },
    True
)

class VERSION_2(VERSION_1):
    pass

version_2 = VERSION_2(
    schema.tables.OTCDaily,
    {   
        '外資及陸資買進金額(元)': schema.tables.OTCDaily.f_foreign_flow_value.外陸資_買進_金額,
        '外資及陸資賣出金額(元)': schema.tables.OTCDaily.f_foreign_flow_value.外陸資_賣出_金額,
        '投信買進金額(元)': schema.tables.OTCDaily.f_trust_flow_value.投信_買進_金額,
        '投信賣出金額(元)': schema.tables.OTCDaily.f_trust_flow_value.投信_賣出_金額,
        '自營商買進金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_買進_金額,
        '自營商賣出金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_賣出_金額,
    },
    True
)

class VERSION_3(FLOW_VALUE):
    def to_df(self, content):
        df = super().to_df(content)
        df = self.flat_df(df,'單位名稱', 18)
        return df

version_3 = VERSION_3(
    schema.tables.OTCDaily,
    {   
        '外資及陸資買進金額(元)': schema.tables.OTCDaily.f_foreign_flow_value.外陸資_買進_金額,
        '外資及陸資賣出金額(元)': schema.tables.OTCDaily.f_foreign_flow_value.外陸資_賣出_金額,
        '投信買進金額(元)': schema.tables.OTCDaily.f_trust_flow_value.投信_買進_金額,
        '投信賣出金額(元)': schema.tables.OTCDaily.f_trust_flow_value.投信_賣出_金額,
        '自營商(自行買賣)買進金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_自行買賣_買進_金額,
        '自營商(自行買賣)賣出金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_自行買賣_賣出_金額,
        '自營商(避險)買進金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_避險_買進_金額,
        '自營商(避險)賣出金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_避險_賣出_金額,
        '自營商買進金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_買進_金額,
        '自營商賣出金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_賣出_金額,
    },
    True
)

class VERSION_4(FLOW_VALUE):
    def to_df(self, content):
        df = super().to_df(content)
        df = self.flat_df(df,'單位名稱', 12)
        return df

version_4 = VERSION_4(
    schema.tables.OTCDaily,
    {   
        '投信買進金額(元)': schema.tables.OTCDaily.f_trust_flow_value.投信_買進_金額,
        '投信賣出金額(元)': schema.tables.OTCDaily.f_trust_flow_value.投信_賣出_金額,
        '自營商(自行買賣)買進金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_自行買賣_買進_金額,
        '自營商(自行買賣)賣出金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_自行買賣_賣出_金額,
        '自營商(避險)買進金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_避險_買進_金額,
        '自營商(避險)賣出金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_避險_賣出_金額,
        '自營商買進金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_買進_金額,
        '自營商賣出金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_賣出_金額,
    },
    True
)

class VERSION_5(FLOW_VALUE):
    def to_df(self, content):
        df = super().to_df(content)
        df = self.flat_df(df,'單位名稱', 24)
        return df

version_5 = VERSION_5(
    schema.tables.OTCDaily,
    {
        '外資及陸資(不含自營商)買進金額(元)': schema.tables.OTCDaily.f_foreign_flow_value.外陸資_不含外資自營商_買進_金額,
        '外資及陸資(不含自營商)賣出金額(元)': schema.tables.OTCDaily.f_foreign_flow_value.外陸資_不含外資自營商_賣出_金額,
        '外資自營商買進金額(元)': schema.tables.OTCDaily.f_foreign_flow_value.外資自營商_買進_金額,
        '外資自營商賣出金額(元)': schema.tables.OTCDaily.f_foreign_flow_value.外資自營商_賣出_金額,
        '外資及陸資合計買進金額(元)': schema.tables.OTCDaily.f_foreign_flow_value.外陸資_買進_金額,
        '外資及陸資合計賣出金額(元)': schema.tables.OTCDaily.f_foreign_flow_value.外陸資_賣出_金額,
        '投信買進金額(元)': schema.tables.OTCDaily.f_trust_flow_value.投信_買進_金額,
        '投信賣出金額(元)': schema.tables.OTCDaily.f_trust_flow_value.投信_賣出_金額,
        '自營商(自行買賣)買進金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_自行買賣_買進_金額,
        '自營商(自行買賣)賣出金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_自行買賣_賣出_金額,
        '自營商(避險)買進金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_避險_買進_金額,
        '自營商(避險)賣出金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_避險_賣出_金額,
        '自營商合計買進金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_買進_金額,
        '自營商合計賣出金額(元)': schema.tables.OTCDaily.f_dealer_flow_value.自營商_賣出_金額,
    },
    True
)
