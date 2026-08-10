from ..base import TWSE_MARKET_V0
from .....schema.tables import TWSEDaily

class VERSION_0(TWSE_MARKET_V0):
        
    def format_dtype(self, df):
        taiwan_date_cols = [
            TWSEDaily.f_datatimestamp.資料日期
        ]
        int_cols = [
            TWSEDaily.f_technicals_volume.交易股數,
            TWSEDaily.f_technicals_volume.交易筆數,
            TWSEDaily.f_technicals_volume.交易金額
        ]
        float_cols = [
            TWSEDaily.f_technicals_price.收盤價
        ]
        df = super().format_dtype(df, int_cols=int_cols, float_cols=float_cols, taiwan_date_cols=taiwan_date_cols)
        return df

version_0 = VERSION_0(
    TWSEDaily,
    {
        '日期': TWSEDaily.f_datatimestamp.資料日期,
        '發行量加權股價指數': TWSEDaily.f_technicals_price.收盤價,
        '成交股數': TWSEDaily.f_technicals_volume.交易股數,
        '成交金額': TWSEDaily.f_technicals_volume.交易金額,
        '成交筆數': TWSEDaily.f_technicals_volume.交易筆數
    },
    False
)
