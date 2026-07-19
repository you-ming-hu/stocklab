from ..... import schema

from ..stocks import short_sbl_volume as base

class VERSION_0(base.VERSION_0):
    
    def format_dtype(self, df):
        for name in df.columns:
            df[name] = df[name].str.replace(',','').astype(int)
        df = df.sum(axis=0).to_frame().T
        return df
    
version_0 = VERSION_0(
    schema.tables.TWSEDaily,
    {
        '融券賣出': schema.tables.TWSEDaily.f_short_flow_volume.融券_賣出_股數,
        '融券買進': schema.tables.TWSEDaily.f_short_flow_volume.融券_買進_股數,
        '融券現券': schema.tables.TWSEDaily.f_short_flow_volume.融券_現償_股數,
        '融券今日餘額': schema.tables.TWSEDaily.f_short_balance_volume.融券_餘額_股數,
        '借券賣出賣出': schema.tables.TWSEDaily.f_sbl_flow_volume.借券賣出_賣出_股數,
        '借券賣出庫存異動': schema.tables.TWSEDaily.f_sbl_flow_volume.借券賣出_不含賣出_總異動_股數,
        '借券賣出今日餘額': schema.tables.TWSEDaily.f_sbl_balance_volume.借券賣出_餘額_股數,
    },
    True
)

class VERSION_1(base.VERSION_1):
    
    def format_dtype(self, df):
        for name in df.columns:
            df[name] = df[name].str.replace(',','').astype(int)
        df = df.sum(axis=0).to_frame().T
        return df
    
version_1 = VERSION_1(
    schema.tables.TWSEDaily,
    {
        '融券賣出': schema.tables.TWSEDaily.f_short_flow_volume.融券_賣出_股數,
        '融券買進': schema.tables.TWSEDaily.f_short_flow_volume.融券_買進_股數,
        '融券現券': schema.tables.TWSEDaily.f_short_flow_volume.融券_現償_股數,
        '融券今日餘額': schema.tables.TWSEDaily.f_short_balance_volume.融券_餘額_股數,
        '借券賣出當日賣出': schema.tables.TWSEDaily.f_sbl_flow_volume.借券賣出_賣出_股數,
        '借券賣出當日還券': schema.tables.TWSEDaily.f_sbl_flow_volume.借券賣出_還券_股數,
        '借券賣出當日調整': schema.tables.TWSEDaily.f_sbl_flow_volume.借券賣出_調整_股數,
        '借券賣出當日餘額': schema.tables.TWSEDaily.f_sbl_balance_volume.借券賣出_餘額_股數,
    },
    True
)