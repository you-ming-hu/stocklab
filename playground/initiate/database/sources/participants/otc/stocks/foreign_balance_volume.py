# from ..base import BALANCE_VOLUME
# from ..... import schema

# class BASE(BALANCE_VOLUME):
#     def format_dtype(self, df):
#         stock_info_cols = [
#             schema.tables.StockDaily.f_stock_info.代號
#         ]
#         volume_cols = [
#             schema.tables.StockDaily.f_stock_info.總發行股數,
#             schema.tables.StockDaily.f_foreign_balance_volume.外陸資_餘額_股數,
#         ]
#         ratio_cols = [
#             schema.tables.StockDaily.f_foreign_limit.外陸資_投資上限_比率
#         ]
#         super().format_dtype(df, stock_info_cols, volume_cols, ratio_cols)
#         return df

# class VERSION_0(BASE):
#     def to_df(self, content):
#         df = super().to_df(content, 11)
#         return df

# version_0 = VERSION_0(
#     schema.tables.StockDaily,
#     {
#         '證券代號': schema.tables.StockDaily.f_stock_info.代號,
#         '發行股數': schema.tables.StockDaily.f_stock_info.總發行股數,
#         '全體外資持有股數': schema.tables.StockDaily.f_foreign_balance_volume.外陸資_餘額_股數,
#         '法令投資上限比率': schema.tables.StockDaily.f_foreign_limit.外陸資_投資上限_比率,
#     },
#     True
# )

# class VERSION_1(BASE):
#     def to_df(self, content):
#         df = super().to_df(content, 12)
#         return df

# version_1 = VERSION_1(
#     schema.tables.StockDaily,
#     {
#         '證券代號': schema.tables.StockDaily.f_stock_info.代號,
#         '發行股數': schema.tables.StockDaily.f_stock_info.總發行股數,
#         '全體外資及陸資持有股數': schema.tables.StockDaily.f_foreign_balance_volume.外陸資_餘額_股數,
#         '外資及陸資共用法令投資上限比率': schema.tables.StockDaily.f_foreign_limit.外陸資_投資上限_比率,
#     },
#     True
# )