from ..... import schema
from ..stocks import foreign_trust_dealer_volume as base

class VERSION_0(base.VERSION_0):
    def format_dtype(self, df):
        for name in df.columns:
            df[name] = df[name].str.replace(',','').astype(int)
        df = df.sum(axis=0).to_frame().T
        return df
    
version_0 = VERSION_0(
    schema.tables.TWSEDaily,
    {
        '外資買進股數': schema.tables.TWSEDaily.f_institution_share_flow.外陸資買進股數,
        '外資賣出股數': schema.tables.TWSEDaily.f_institution_share_flow.外陸資賣出股數,
        '投信買進股數': schema.tables.TWSEDaily.f_institution_share_flow.投信買進股數,
        '投信賣出股數': schema.tables.TWSEDaily.f_institution_share_flow.投信賣出股數,
        '自營商買進股數': schema.tables.TWSEDaily.f_institution_share_flow.自營商買進股數,
        '自營商賣出股數': schema.tables.TWSEDaily.f_institution_share_flow.自營商賣出股數
    },
    True
)
    
class VERSION_1(VERSION_0, base.VERSION_1):
    pass

version_1 = VERSION_1(
    schema.tables.TWSEDaily,
    {
        '外資買進股數': schema.tables.TWSEDaily.f_institution_share_flow.外陸資買進股數,
        '外資賣出股數': schema.tables.TWSEDaily.f_institution_share_flow.外陸資賣出股數,
        '投信買進股數': schema.tables.TWSEDaily.f_institution_share_flow.投信買進股數,
        '投信賣出股數': schema.tables.TWSEDaily.f_institution_share_flow.投信賣出股數,
        '自營商買進股數(自行買賣)': schema.tables.TWSEDaily.f_institution_share_flow.自營商_自行買賣_買進股數,
        '自營商賣出股數(自行買賣)': schema.tables.TWSEDaily.f_institution_share_flow.自營商_自行買賣_賣出股數,
        '自營商買進股數(避險)': schema.tables.TWSEDaily.f_institution_share_flow.自營商_避險_買進股數,
        '自營商賣出股數(避險)': schema.tables.TWSEDaily.f_institution_share_flow.自營商_避險_賣出股數,
        
    },
    True
)

class VERSION_2(VERSION_0, base.VERSION_2):
    pass

version_2 = VERSION_2(
    schema.tables.TWSEDaily,
    {   
        '外陸資買進股數(不含外資自營商)': schema.tables.TWSEDaily.f_institution_share_flow.外陸資_不含外資自營商_買進股數,
        '外陸資賣出股數(不含外資自營商)': schema.tables.TWSEDaily.f_institution_share_flow.外陸資_不含外資自營商_賣出股數,
        '外資自營商買進股數': schema.tables.TWSEDaily.f_institution_share_flow.外資自營商買進股數,
        '外資自營商賣出股數': schema.tables.TWSEDaily.f_institution_share_flow.外資自營商賣出股數,
        '投信買進股數': schema.tables.TWSEDaily.f_institution_share_flow.投信買進股數,
        '投信賣出股數': schema.tables.TWSEDaily.f_institution_share_flow.投信賣出股數,
        '自營商買進股數(自行買賣)': schema.tables.TWSEDaily.f_institution_share_flow.自營商_自行買賣_買進股數,
        '自營商賣出股數(自行買賣)': schema.tables.TWSEDaily.f_institution_share_flow.自營商_自行買賣_賣出股數,
        '自營商買進股數(避險)': schema.tables.TWSEDaily.f_institution_share_flow.自營商_避險_買進股數,
        '自營商賣出股數(避險)': schema.tables.TWSEDaily.f_institution_share_flow.自營商_避險_賣出股數,
    },
    True
)
