from ..... import schema

from ..base import SHORT_SBL_VALUE

class VERSION_0(SHORT_SBL_VALUE):
        
    def format_dtype(self, df):
        stock_info_cols= [
            schema.tables.StockDaily.f_stock_info.代號,
        ]
        volume_cols = [
            schema.tables.StockDaily.f_short_flow_value.融券_賣出_金額,
            schema.tables.StockDaily.f_sbl_flow_value.借券賣出_賣出_金額
        ]
        for name in stock_info_cols:
            df[name] = df[name].str.replace(' ','').replace('*','')
        for name in volume_cols:
            df[name] = df[name].str.replace(',','').astype(int)
        return df

version_0 = VERSION_0(
    schema.tables.StockDaily,
    {   
        '代號': schema.tables.StockDaily.f_stock_info.代號, 
        '融券賣出成交金額(元)': schema.tables.StockDaily.f_short_flow_value.融券_賣出_金額, 
        '借券賣出成交金額(元)': schema.tables.StockDaily.f_sbl_flow_value.借券賣出_賣出_金額
    },
    True
)