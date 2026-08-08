from ..base import TWSE_STOCKS
from ..... import schema

class VERSION_0(TWSE_STOCKS):
    
    def check_empty(self, content):
        return not 'tables' in content
    
    def to_df(self, content):
        target_table = None
        for table in content['tables']:
            if '每日收盤行情' in table.get('title',''):
                target_table = table
        assert target_table is not None
        df = super().to_df(target_table)
        return df

version_0 = VERSION_0(
    schema.tables.StockDaily,
    {
        '證券代號': schema.tables.StockDaily.f_stock_info.代號, 
        '證券名稱': schema.tables.StockDaily.f_stock_info.名稱,
        '開盤價': schema.tables.StockDaily.f_technicals_price.開盤價,
        '最高價': schema.tables.StockDaily.f_technicals_price.最高價,
        '最低價': schema.tables.StockDaily.f_technicals_price.最低價,
        '收盤價': schema.tables.StockDaily.f_technicals_price.收盤價,
        '成交股數': schema.tables.StockDaily.f_technicals_volume.交易股數,
        '成交筆數': schema.tables.StockDaily.f_technicals_volume.交易筆數,
        '成交金額': schema.tables.StockDaily.f_technicals_volume.交易金額,
    },
    True
)