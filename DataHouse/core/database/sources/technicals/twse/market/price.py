from ..base import TWSE_MARKET_V0
from .....schema.tables import TWSEDaily

class VERSION_0(TWSE_MARKET_V0):
        
    def format_dtype(self, df):
        taiwan_date_cols = [
            TWSEDaily.f_datatimestamp.資料日期
        ]
        float_cols = [
            TWSEDaily.f_technicals_price.開盤價,
            TWSEDaily.f_technicals_price.最高價,
            TWSEDaily.f_technicals_price.最低價,
            TWSEDaily.f_technicals_price.收盤價
        ]
        df = super().format_dtype(df, float_cols=float_cols, taiwan_date_cols=taiwan_date_cols)
        return df

version_0 = VERSION_0(
    TWSEDaily,
    {
        '日期': TWSEDaily.f_datatimestamp.資料日期,
        '開盤指數': TWSEDaily.f_technicals_price.開盤價,
        '最高指數': TWSEDaily.f_technicals_price.最高價,
        '最低指數': TWSEDaily.f_technicals_price.最低價,
        '收盤指數': TWSEDaily.f_technicals_price.收盤價
    },
    False
)