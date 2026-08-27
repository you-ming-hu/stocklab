from ..base import FLOW_VOLUME_V0
from .....schema.tables import StockDaily

class VERSION_0(FLOW_VOLUME_V0):

    def to_df(self, content):
        df = super().to_df(content, 12)
        return df
    
    def format_dtype(self, df):
        str_cols= [
            StockDaily.f_stock_info.代號
        ]
        int_cols = [
            StockDaily.f_foreign_flow_volume.外陸資_買進_股數,
            StockDaily.f_foreign_flow_volume.外陸資_賣出_股數,
            StockDaily.f_trust_flow_volume.投信_買進_股數,
            StockDaily.f_trust_flow_volume.投信_賣出_股數,
            StockDaily.f_dealer_flow_volume.自營商_買進_股數,
            StockDaily.f_dealer_flow_volume.自營商_賣出_股數
        ]
        df = super().format_dtype(df, str_cols, int_cols)
        return df

version_0 = VERSION_0(
    StockDaily,
    {
        '證券代號': StockDaily.f_stock_info.代號,
        '外資買進股數': StockDaily.f_foreign_flow_volume.外陸資_買進_股數,
        '外資賣出股數': StockDaily.f_foreign_flow_volume.外陸資_賣出_股數,
        '投信買進股數': StockDaily.f_trust_flow_volume.投信_買進_股數,
        '投信賣出股數': StockDaily.f_trust_flow_volume.投信_賣出_股數,
        '自營商買進股數': StockDaily.f_dealer_flow_volume.自營商_買進_股數,
        '自營商賣出股數': StockDaily.f_dealer_flow_volume.自營商_賣出_股數
    },
    True
)

class VERSION_1(FLOW_VOLUME_V0):

    def to_df(self, content):
        df = super().to_df(content, 16)
        return df
    
    def format_dtype(self, df):
        str_cols= [
            StockDaily.f_stock_info.代號
        ]
        int_cols = [
            StockDaily.f_foreign_flow_volume.外陸資_買進_股數,
            StockDaily.f_foreign_flow_volume.外陸資_賣出_股數,
            StockDaily.f_trust_flow_volume.投信_買進_股數,
            StockDaily.f_trust_flow_volume.投信_賣出_股數,
            StockDaily.f_dealer_flow_volume.自營商_自行買賣_買進_股數,
            StockDaily.f_dealer_flow_volume.自營商_自行買賣_賣出_股數,
            StockDaily.f_dealer_flow_volume.自營商_避險_買進_股數,
            StockDaily.f_dealer_flow_volume.自營商_避險_賣出_股數
        ]
        df = super().format_dtype(df, str_cols, int_cols)
        return df

    
    def add_other_columns(self, df):
        cols = self.table.columns
        df[cols['自營商_買進_股數']] = df[cols['自營商_自行買賣_買進_股數']] + df[cols['自營商_避險_買進_股數']]
        df[cols['自營商_賣出_股數']] = df[cols['自營商_自行買賣_賣出_股數']] + df[cols['自營商_避險_賣出_股數']]
        return df

version_1 = VERSION_1(
    StockDaily,
    {
        '證券代號': StockDaily.f_stock_info.代號,
        '外資買進股數': StockDaily.f_foreign_flow_volume.外陸資_買進_股數,
        '外資賣出股數': StockDaily.f_foreign_flow_volume.外陸資_賣出_股數,
        '投信買進股數': StockDaily.f_trust_flow_volume.投信_買進_股數,
        '投信賣出股數': StockDaily.f_trust_flow_volume.投信_賣出_股數,
        '自營商買進股數(自行買賣)': StockDaily.f_dealer_flow_volume.自營商_自行買賣_買進_股數,
        '自營商賣出股數(自行買賣)': StockDaily.f_dealer_flow_volume.自營商_自行買賣_賣出_股數,
        '自營商買進股數(避險)': StockDaily.f_dealer_flow_volume.自營商_避險_買進_股數,
        '自營商賣出股數(避險)': StockDaily.f_dealer_flow_volume.自營商_避險_賣出_股數,
    },
    True
)

class VERSION_2(FLOW_VOLUME_V0):
    
    def to_df(self, content):
        df = super().to_df(content, 19)
        return df
    
    def format_dtype(self, df):
        str_cols= [
            StockDaily.f_stock_info.代號
        ]
        int_cols = [
            StockDaily.f_foreign_flow_volume.外陸資_不含外資自營商_買進_股數,
            StockDaily.f_foreign_flow_volume.外陸資_不含外資自營商_賣出_股數,
            StockDaily.f_foreign_flow_volume.外資自營商_買進_股數,
            StockDaily.f_foreign_flow_volume.外資自營商_賣出_股數,
            StockDaily.f_trust_flow_volume.投信_買進_股數,
            StockDaily.f_trust_flow_volume.投信_賣出_股數,
            StockDaily.f_dealer_flow_volume.自營商_自行買賣_買進_股數,
            StockDaily.f_dealer_flow_volume.自營商_自行買賣_賣出_股數,
            StockDaily.f_dealer_flow_volume.自營商_避險_買進_股數,
            StockDaily.f_dealer_flow_volume.自營商_避險_賣出_股數
        ]
        df = super().format_dtype(df, str_cols, int_cols)
        return df
    
    def add_other_columns(self, df):
        cols = self.table.columns
        df[cols['外陸資_買進_股數']] = df[cols['外陸資_不含外資自營商_買進_股數']] + df[cols['外資自營商_買進_股數']]
        df[cols['外陸資_賣出_股數']] = df[cols['外陸資_不含外資自營商_賣出_股數']] + df[cols['外資自營商_賣出_股數']]
        df[cols['自營商_買進_股數']] = df[cols['自營商_自行買賣_買進_股數']] + df[cols['自營商_避險_買進_股數']]
        df[cols['自營商_賣出_股數']] = df[cols['自營商_自行買賣_賣出_股數']] + df[cols['自營商_避險_賣出_股數']]
        return df

version_2 = VERSION_2(
    StockDaily,
    {   
        '證券代號': StockDaily.f_stock_info.代號,
        '外陸資買進股數(不含外資自營商)': StockDaily.f_foreign_flow_volume.外陸資_不含外資自營商_買進_股數,
        '外陸資賣出股數(不含外資自營商)': StockDaily.f_foreign_flow_volume.外陸資_不含外資自營商_賣出_股數,
        '外資自營商買進股數': StockDaily.f_foreign_flow_volume.外資自營商_買進_股數,
        '外資自營商賣出股數': StockDaily.f_foreign_flow_volume.外資自營商_賣出_股數,
        '投信買進股數': StockDaily.f_trust_flow_volume.投信_買進_股數,
        '投信賣出股數': StockDaily.f_trust_flow_volume.投信_賣出_股數,
        '自營商買進股數(自行買賣)': StockDaily.f_dealer_flow_volume.自營商_自行買賣_買進_股數,
        '自營商賣出股數(自行買賣)': StockDaily.f_dealer_flow_volume.自營商_自行買賣_賣出_股數,
        '自營商買進股數(避險)': StockDaily.f_dealer_flow_volume.自營商_避險_買進_股數,
        '自營商賣出股數(避險)': StockDaily.f_dealer_flow_volume.自營商_避險_賣出_股數,
    },
    True
)
