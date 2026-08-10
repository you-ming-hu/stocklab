from .....schema.tables import StockDaily

from ..base import SHORT_SBL_VALUE_V0

class VERSION_0(SHORT_SBL_VALUE_V0):
        
    def format_dtype(self, df):
        str_cols= [
            StockDaily.f_stock_info.代號,
        ]
        int_cols = [
            StockDaily.f_short_flow_value.融券_賣出_金額,
            StockDaily.f_sbl_flow_value.借券賣出_賣出_金額
        ]
        df = super().format_dtype(df, str_cols, int_cols)
        return df

version_0 = VERSION_0(
    StockDaily,
    {   
        '代號': StockDaily.f_stock_info.代號, 
        '融券賣出成交金額(元)': StockDaily.f_short_flow_value.融券_賣出_金額, 
        '借券賣出成交金額(元)': StockDaily.f_sbl_flow_value.借券賣出_賣出_金額
    },
    True
)