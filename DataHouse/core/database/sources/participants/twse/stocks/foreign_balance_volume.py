from ..base import BALANCE_VOLUME_V0
from .....schema.tables import StockDaily

class BASE(BALANCE_VOLUME_V0):

    def format_dtype(self, df):
        str_cols = [
            StockDaily.f_stock_info.代號
        ]
        int_cols = [
            StockDaily.f_stock_info.總發行股數,
            StockDaily.f_foreign_balance_volume.外陸資_餘額_股數,
        ]
        float_cols = [
            StockDaily.f_foreign_limit.外陸資_投資上限_比率
        ]
        df = super().format_dtype(df, str_cols, int_cols, float_cols)
        return df

class VERSION_0(BASE):

    def to_df(self, content):
        df = super().to_df(content, 11)
        return df

version_0 = VERSION_0(
    StockDaily,
    {
        '證券代號': StockDaily.f_stock_info.代號,
        '發行股數': StockDaily.f_stock_info.總發行股數,
        '全體外資持有股數': StockDaily.f_foreign_balance_volume.外陸資_餘額_股數,
        '法令投資上限比率': StockDaily.f_foreign_limit.外陸資_投資上限_比率,
    },
    True
)

class VERSION_1(BASE):
    
    def to_df(self, content):
        df = super().to_df(content, 12)
        return df

version_1 = VERSION_1(
    StockDaily,
    {
        '證券代號': StockDaily.f_stock_info.代號,
        '發行股數': StockDaily.f_stock_info.總發行股數,
        '全體外資及陸資持有股數': StockDaily.f_foreign_balance_volume.外陸資_餘額_股數,
        '外資及陸資共用法令投資上限比率': StockDaily.f_foreign_limit.外陸資_投資上限_比率,
    },
    True
)