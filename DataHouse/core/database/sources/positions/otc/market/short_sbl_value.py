from ..... import schema

from ..base import SUM

from ..stocks import short_sbl_value as base

class VERSION_0(SUM, base.VERSION_0):
    pass
    
version_0 = VERSION_0(
    schema.tables.OTCDaily,
    {   
        '融券賣出成交金額(元)': schema.tables.StockDaily.f_short_flow_value.融券_賣出_金額, 
        '借券賣出成交金額(元)': schema.tables.StockDaily.f_sbl_flow_value.借券賣出_賣出_金額
    },
    True
)