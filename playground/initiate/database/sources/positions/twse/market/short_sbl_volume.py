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
        '融券賣出': schema.tables.TWSEDaily.f_short.融券賣出股數,
        '融券買進': schema.tables.TWSEDaily.f_short.融券買進股數,
        '融券現券': schema.tables.TWSEDaily.f_short.融券現償股數,
        '融券今日餘額': schema.tables.TWSEDaily.f_short.融券餘額股數,
        '借券賣出賣出': schema.tables.TWSEDaily.f_short.借券賣出賣出股數,
        '借券賣出庫存異動': schema.tables.TWSEDaily.f_short.借券賣出不含賣出總異動股數,
        '借券賣出今日餘額': schema.tables.TWSEDaily.f_short.借券賣出餘額股數,
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
        '融券賣出': schema.tables.TWSEDaily.f_short.融券賣出股數,
        '融券買進': schema.tables.TWSEDaily.f_short.融券買進股數,
        '融券現券': schema.tables.TWSEDaily.f_short.融券現償股數,
        '融券今日餘額': schema.tables.TWSEDaily.f_short.融券餘額股數,
        '借券賣出當日賣出': schema.tables.TWSEDaily.f_short.借券賣出賣出股數,
        '借券賣出當日還券': schema.tables.TWSEDaily.f_short.借券賣出還券股數,
        '借券賣出當日調整': schema.tables.TWSEDaily.f_short.借券賣出調整股數,
        '借券賣出當日餘額': schema.tables.TWSEDaily.f_short.借券賣出餘額股數,
    },
    True
)