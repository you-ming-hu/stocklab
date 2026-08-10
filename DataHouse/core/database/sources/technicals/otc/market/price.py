from ..base import OTC_MARKET_V0
from ..... import schema

class VERSION_0(OTC_MARKET_V0):
        
    def format_dtype(self, df):
        date_cols = [
            schema.tables.OTCDaily.f_datatimestamp.資料日期
        ]
        float_cols = [
            schema.tables.OTCDaily.f_technicals_price.開盤價,
            schema.tables.OTCDaily.f_technicals_price.最高價,
            schema.tables.OTCDaily.f_technicals_price.最低價,
            schema.tables.OTCDaily.f_technicals_price.收盤價
        ]
        df = super().format_dtype(df, float_cols=float_cols, date_cols=date_cols)
        return df

version_0 = VERSION_0(
    schema.tables.OTCDaily,
    {
        '日期': schema.tables.OTCDaily.f_datatimestamp.資料日期,
        '開市': schema.tables.OTCDaily.f_technicals_price.開盤價,
        '最高': schema.tables.OTCDaily.f_technicals_price.最高價,
        '最低': schema.tables.OTCDaily.f_technicals_price.最低價,
        '收市': schema.tables.OTCDaily.f_technicals_price.收盤價
    },
    False
)