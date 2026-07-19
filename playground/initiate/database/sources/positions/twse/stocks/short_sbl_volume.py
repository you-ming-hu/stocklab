from ..... import schema

from ..base import SHORT_SBL

class VERSION_0(SHORT_SBL):
    
    def to_df(self, content):
        df = super().to_df(content, 14)
        return df
        
    def format_dtype(self, df):
        stock_info_cols = [
            schema.tables.StockDaily.f_stock_info.代號,
        ]
        volume_cols = [
            schema.tables.StockDaily.f_short_flow_volume.融券_賣出_股數,
            schema.tables.StockDaily.f_short_flow_volume.融券_買進_股數,
            schema.tables.StockDaily.f_short_flow_volume.融券_現償_股數,
            schema.tables.StockDaily.f_short_balance_volume.融券_餘額_股數,
            schema.tables.StockDaily.f_short_limit.融券_次日限額_股數,
            schema.tables.StockDaily.f_sbl_flow_volume.借券賣出_賣出_股數,
            schema.tables.StockDaily.f_sbl_flow_volume.借券賣出_不含賣出_總異動_股數,
            schema.tables.StockDaily.f_sbl_balance_volume.借券賣出_餘額_股數,
            schema.tables.StockDaily.f_sbl_limit.借券賣出_次日限額_股數
        ]
        df = super().format_dtype(df, stock_info_cols, volume_cols)
        return df

version_0 = VERSION_0(
    schema.tables.StockDaily,
    {
        '股票代號': schema.tables.StockDaily.f_stock_info.代號,
        '融券賣出': schema.tables.StockDaily.f_short_flow_volume.融券_賣出_股數,
        '融券買進': schema.tables.StockDaily.f_short_flow_volume.融券_買進_股數,
        '融券現券': schema.tables.StockDaily.f_short_flow_volume.融券_現償_股數,
        '融券今日餘額': schema.tables.StockDaily.f_short_balance_volume.融券_餘額_股數,
        '融券次一營業日限額': schema.tables.StockDaily.f_short_limit.融券_次日限額_股數,
        '借券賣出賣出': schema.tables.StockDaily.f_sbl_flow_volume.借券賣出_賣出_股數,
        '借券賣出庫存異動': schema.tables.StockDaily.f_sbl_flow_volume.借券賣出_不含賣出_總異動_股數,
        '借券賣出今日餘額': schema.tables.StockDaily.f_sbl_balance_volume.借券賣出_餘額_股數,
        '借券賣出可使用額度': schema.tables.StockDaily.f_sbl_limit.借券賣出_次日限額_股數
    },
    True
)

class VERSION_1(SHORT_SBL):
    
    def to_df(self, content):
        df = super().to_df(content, 15)
        return df
        
    def format_dtype(self, df):
        stock_info_cols = [
            schema.tables.StockDaily.f_stock_info.代號,
        ]
        volume_cols = [
            schema.tables.StockDaily.f_short_flow_volume.融券_賣出_股數,
            schema.tables.StockDaily.f_short_flow_volume.融券_買進_股數,
            schema.tables.StockDaily.f_short_flow_volume.融券_現償_股數,
            schema.tables.StockDaily.f_short_balance_volume.融券_餘額_股數,
            schema.tables.StockDaily.f_short_limit.融券_次日限額_股數,
            schema.tables.StockDaily.f_sbl_flow_volume.借券賣出_賣出_股數,
            schema.tables.StockDaily.f_sbl_flow_volume.借券賣出_還券_股數,
            schema.tables.StockDaily.f_sbl_flow_volume.借券賣出_調整_股數,
            schema.tables.StockDaily.f_sbl_balance_volume.借券賣出_餘額_股數,
            schema.tables.StockDaily.f_sbl_limit.借券賣出_次日限額_股數
        ]
        df = super().format_dtype(df, stock_info_cols, volume_cols)
        return df
    
    def add_other_columns(self, df):
        cols = self.table.columns
        df[cols['借券賣出_不含賣出_總異動_股數']] = df[cols['借券賣出_調整_股數']] - df[cols['借券賣出_還券_股數']]
        return df
    
version_1 = VERSION_1(
    schema.tables.StockDaily,
    {
        '股票代號': schema.tables.StockDaily.f_stock_info.代號,
        '融券賣出': schema.tables.StockDaily.f_short_flow_volume.融券_賣出_股數,
        '融券買進': schema.tables.StockDaily.f_short_flow_volume.融券_買進_股數,
        '融券現券': schema.tables.StockDaily.f_short_flow_volume.融券_現償_股數,
        '融券今日餘額': schema.tables.StockDaily.f_short_balance_volume.融券_餘額_股數,
        '融券次一營業日限額': schema.tables.StockDaily.f_short_limit.融券_次日限額_股數,
        '借券賣出當日賣出': schema.tables.StockDaily.f_sbl_flow_volume.借券賣出_賣出_股數,
        '借券賣出當日還券': schema.tables.StockDaily.f_sbl_flow_volume.借券賣出_還券_股數,
        '借券賣出當日調整': schema.tables.StockDaily.f_sbl_flow_volume.借券賣出_調整_股數,
        '借券賣出當日餘額': schema.tables.StockDaily.f_sbl_balance_volume.借券賣出_餘額_股數,
        '借券賣出次一營業日可限額': schema.tables.StockDaily.f_sbl_limit.借券賣出_次日限額_股數
    },
    True
)