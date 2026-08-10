from ..base import OTC_MARKET_V0
from .....schema.tables import OTCDaily

class VERSION_0(OTC_MARKET_V0):
        
    def format_dtype(self, df):
        taiwan_date_cols = [
            OTCDaily.f_datatimestamp.資料日期
        ]
        int_cols = [
            OTCDaily.f_technicals_volume.交易股數,
            OTCDaily.f_technicals_volume.交易金額,
            OTCDaily.f_technicals_volume.交易筆數,
        ]
        float_cols = [
            OTCDaily.f_technicals_price.收盤價
        ]
        df = super().format_dtype(df, int_cols=int_cols, float_cols=float_cols, taiwan_date_cols=taiwan_date_cols)

        thousand_cols = int_cols[:2]
        df[thousand_cols] = df[thousand_cols] * 1000
        return df

version_0 = VERSION_0(
    OTCDaily,
    {
        '日期': OTCDaily.f_datatimestamp.資料日期,
        '櫃買指數': OTCDaily.f_technicals_price.收盤價,
        '成交股數（仟股）': OTCDaily.f_technicals_volume.交易股數,
        '金額（仟元）': OTCDaily.f_technicals_volume.交易金額,
        '筆數': OTCDaily.f_technicals_volume.交易筆數
    },
    False
)

class VERSION_1(VERSION_0):
    pass

version_1 = VERSION_1(
    OTCDaily,
    {
        '日期': OTCDaily.f_datatimestamp.資料日期,
        '櫃買指數': OTCDaily.f_technicals_price.收盤價,
        '成交張數': OTCDaily.f_technicals_volume.交易股數,
        '金額（仟元）': OTCDaily.f_technicals_volume.交易金額,
        '筆數': OTCDaily.f_technicals_volume.交易筆數
    },
    False
)
