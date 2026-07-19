from ... import schema

from ..base import Source

class STOCKS(Source):

    def add_other_columns(self, df):
        df[schema.tables.StockDaily.f_stock_info.市場別] = self.__class__.market_type
        df[schema.tables.StockDaily.f_stock_info.交易中] = True
        return df