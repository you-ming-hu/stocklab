from .....schema.tables import StockDaily

from ..base import SHORT_SBL_VOLUME_V0

import pandas as pd

class VERSION_0(SHORT_SBL_VOLUME_V0):
        
    def format_dtype(self, df):
        str_cols = [
            StockDaily.f_stock_info.代號,
        ]
        int_cols = [
            StockDaily.f_short_flow_volume.融券_賣出_股數,
            StockDaily.f_short_flow_volume.融券_買進_股數,
            StockDaily.f_short_flow_volume.融券_現償_股數,
            StockDaily.f_short_balance_volume.融券_餘額_股數,
            StockDaily.f_short_limit.融券_次日限額_股數,
            StockDaily.f_sbl_flow_volume.借券賣出_賣出_股數,
            StockDaily.f_sbl_flow_volume.借券賣出_不含賣出_總異動_股數,
            StockDaily.f_sbl_balance_volume.借券賣出_餘額_股數,
            StockDaily.f_sbl_limit.借券賣出_次日限額_股數
        ]
        df = super().format_dtype(df, str_cols, int_cols)
        return df

version_0 = VERSION_0(
    StockDaily,
    {
        '股票代號': StockDaily.f_stock_info.代號,
        '融券賣出': StockDaily.f_short_flow_volume.融券_賣出_股數,
        '融券買進': StockDaily.f_short_flow_volume.融券_買進_股數,
        '融券現券': StockDaily.f_short_flow_volume.融券_現償_股數,
        '融券今日餘額': StockDaily.f_short_balance_volume.融券_餘額_股數,
        '融券限額': StockDaily.f_short_limit.融券_次日限額_股數,
        '借券賣出': StockDaily.f_sbl_flow_volume.借券賣出_賣出_股數,
        '借券庫存異動': StockDaily.f_sbl_flow_volume.借券賣出_不含賣出_總異動_股數,
        '借券今日餘額': StockDaily.f_sbl_balance_volume.借券賣出_餘額_股數,
        '借券可使用額度': StockDaily.f_sbl_limit.借券賣出_次日限額_股數
    },
    True
)

class VERSION_1(SHORT_SBL_VOLUME_V0):
    
    def format_dtype(self, df):
        str_cols = [
            StockDaily.f_stock_info.代號,
        ]
        int_cols = [
            StockDaily.f_short_flow_volume.融券_賣出_股數,
            StockDaily.f_short_flow_volume.融券_買進_股數,
            StockDaily.f_short_flow_volume.融券_現償_股數,
            StockDaily.f_short_balance_volume.融券_餘額_股數,
            StockDaily.f_short_limit.融券_次日限額_股數,
            StockDaily.f_sbl_flow_volume.借券賣出_賣出_股數,
            StockDaily.f_sbl_flow_volume.借券賣出_還券_股數,
            StockDaily.f_sbl_flow_volume.借券賣出_調整_股數,
            StockDaily.f_sbl_balance_volume.借券賣出_餘額_股數,
            StockDaily.f_sbl_limit.借券賣出_次日限額_股數
        ]
        df = super().format_dtype(df, str_cols, int_cols)
        return df
    
    def add_other_columns(self, df):
        cols = self.table.columns
        df[cols['借券賣出_不含賣出_總異動_股數']] = df[cols['借券賣出_調整_股數']] - df[cols['借券賣出_還券_股數']]
        return df
    
version_1 = VERSION_1(
    StockDaily,
    {
        '股票代號': StockDaily.f_stock_info.代號,
        '融券賣出': StockDaily.f_short_flow_volume.融券_賣出_股數,
        '融券買進': StockDaily.f_short_flow_volume.融券_買進_股數,
        '融券現券': StockDaily.f_short_flow_volume.融券_現償_股數,
        '融券今日餘額': StockDaily.f_short_balance_volume.融券_餘額_股數,
        '融券限額': StockDaily.f_short_limit.融券_次日限額_股數,
        '借券當日賣出': StockDaily.f_sbl_flow_volume.借券賣出_賣出_股數,
        '借券當日還券': StockDaily.f_sbl_flow_volume.借券賣出_還券_股數,
        '借券當日調整數額': StockDaily.f_sbl_flow_volume.借券賣出_調整_股數,
        '借券當日餘額': StockDaily.f_sbl_balance_volume.借券賣出_餘額_股數,
        '今日可借券賣出限額': StockDaily.f_sbl_limit.借券賣出_次日限額_股數
    },
    True
)

class VERSION_2(VERSION_1):

    def to_df(self, content):
        df = super().to_df(content)
        df.columns = pd.Index(['']*2+['融券']*6+['借券賣出']*6+['']) + df.columns
        return df

version_2 = VERSION_2(
    StockDaily,
    {
        '股票代號': StockDaily.f_stock_info.代號,
        '融券賣出': StockDaily.f_short_flow_volume.融券_賣出_股數,
        '融券買進': StockDaily.f_short_flow_volume.融券_買進_股數,
        '融券現券': StockDaily.f_short_flow_volume.融券_現償_股數,
        '融券當日餘額': StockDaily.f_short_balance_volume.融券_餘額_股數,
        '融券限額': StockDaily.f_short_limit.融券_次日限額_股數,
        '借券賣出當日賣出': StockDaily.f_sbl_flow_volume.借券賣出_賣出_股數,
        '借券賣出當日還券': StockDaily.f_sbl_flow_volume.借券賣出_還券_股數,
        '借券賣出當日調整數額': StockDaily.f_sbl_flow_volume.借券賣出_調整_股數,
        '借券賣出當日餘額': StockDaily.f_sbl_balance_volume.借券賣出_餘額_股數,
        '借券賣出次一營業日可借券賣出限額': StockDaily.f_sbl_limit.借券賣出_次日限額_股數
    },
    True
)