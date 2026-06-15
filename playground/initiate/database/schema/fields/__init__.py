import datetime

from . import sql
from .base import Field

class UpdateTimestamp(Field):
    __chinese__ = '更新時間戳記欄位'
    更新日期 = 'update_time', (datetime.date, sql.dtype.str)

class DataTimestamp(Field):
    __chinese__ = '資料時間戳記欄位'
    添加日期 = 'add_time', (datetime.date, sql.dtype.str)
    資料日期 = 'date', (datetime.date, sql.dtype.str)

class StockInfo(Field):
    __chinese__ = '股票資訊'
    代號 = 'id', (str, sql.dtype.str)
    名稱 = 'name', (str, sql.dtype.str)
    市場別 = 'market', (str, sql.dtype.str)

class Technicals(Field):
    __chinese__ = '技術分析'
    開盤價 = 'open', (float, sql.dtype.float)
    最高價 = 'high', (float, sql.dtype.float)
    最低價 = 'low', (float, sql.dtype.float)
    收盤價 = 'close', (float, sql.dtype.float)
    交易股數 = 'volume', (int, sql.dtype.int)
    交易金額 = 'turnover', (int, sql.dtype.int)
    交易筆數 = 'trades', (int, sql.dtype.int)

class Margin(Field):
    __chinese__ = '融資'
    融資買進股數 = 'margin_buy_shares', (int, sql.dtype.int)
    融資賣出股數 = 'margin_sell_shares', (int, sql.dtype.int)
    融資現償股數 = 'margin_cash_repayment_shares', (int, sql.dtype.int)
    融資餘額股數 = 'margin_balance_shares', (int, sql.dtype.int)

class MarginAdditional(Field):
    __chinese__ = '融資額外資訊'
    融資買進金額 = 'margin_buy_amount', (int, sql.dtype.int)
    融資賣出金額 = 'margin_sell_amount', (int, sql.dtype.int)
    融資現償金額 = 'margin_cash_repayment_amount', (int, sql.dtype.int)
    融資餘額金額 = 'margin_balance_amount', (int, sql.dtype.int)

class Short(Field):
    __chinese__ = '融券,借券賣出'

    融券買進股數 = 'short_cover_shares', (int, sql.dtype.int)
    融券賣出股數 = 'short_sell_shares', (int, sql.dtype.int)
    融券現償股數 = 'short_stock_repayment_shares', (int, sql.dtype.int)
    融券餘額股數 = 'short_balance_shares', (int, sql.dtype.int)

    借券賣出賣出股數 = 'slb_sell_shares', (int, sql.dtype.int)
    借券賣出還券股數 = 'slb_return_shares', (int, sql.dtype.int)
    借券賣出調整股數 = 'slb_adjustment_shares', (int, sql.dtype.int)
    借券賣出餘額股數 = 'slb_balance_shares', (int, sql.dtype.int)

class ShortAdditional(Field):
    __chinese__ = '融券,借券賣出額外資訊'
    融券次日限額股數 = 'short_next_day_limit_shares', (int, sql.dtype.int)
    借券賣出次日限額股數 = 'slb_next_day_limit_shares', (int, sql.dtype.int)

class InstitutionFundFlow(Field):
    __chinese__ = '三大法人資金流動'
    外陸資_不含外資自營商_買進金額 = 'foreign_ex_dealer_buy_amount', (int, sql.dtype.int)
    外陸資_不含外資自營商_賣出金額 = 'foreign_ex_dealer_sell_amount', (int, sql.dtype.int)
    外資自營商買進金額 = 'foreign_dealer_buy_amount', (int, sql.dtype.int)
    外資自營商賣出金額 = 'foreign_dealer_sell_amount', (int, sql.dtype.int)
    外陸資買進金額 = 'foreign_buy_amount', (int, sql.dtype.int)
    外陸資賣出金額 = 'foreign_sell_amount', (int, sql.dtype.int)
    投信買進金額 = 'trust_buy_amount', (int, sql.dtype.int)
    投信賣出金額 = 'trust_sell_amount', (int, sql.dtype.int)
    自營商_自行買賣_買進金額 = 'dealer_proprietary_buy_amount', (int, sql.dtype.int)
    自營商_自行買賣_賣出金額 = 'dealer_proprietary_sell_amount', (int, sql.dtype.int)
    自營商_避險_買進金額 = 'dealer_hedge_buy_amount', (int, sql.dtype.int)
    自營商_避險_賣出金額 = 'dealer_hedge_sell_amount', (int, sql.dtype.int)
    自營商買進金額 = 'dealer_buy_amount', (int, sql.dtype.int)
    自營商賣出金額 = 'dealer_sell_amount', (int, sql.dtype.int)

class InstitutionShareFlow(Field):
    __chinese__ = '三大法人持股流動'
    外陸資_不含外資自營商_買進股數 = 'foreign_ex_dealer_buy_shares', (int, sql.dtype.int)
    外陸資_不含外資自營商_賣出股數 = 'foreign_ex_dealer_sell_shares', (int, sql.dtype.int)
    外資自營商買進股數 = 'foreign_dealer_buy_shares', (int, sql.dtype.int)
    外資自營商賣出股數 = 'foreign_dealer_sell_shares', (int, sql.dtype.int)
    外陸資買進股數 = 'foreign_buy_shares', (int, sql.dtype.int)
    外陸資賣出股數 = 'foreign_sell_shares', (int, sql.dtype.int)
    投信買進股數 = 'trust_buy_shares', (int, sql.dtype.int)
    投信賣出股數 = 'trust_sell_shares', (int, sql.dtype.int)
    自營商_自行買賣_買進股數 = 'dealer_proprietary_buy_shares', (int, sql.dtype.int)
    自營商_自行買賣_賣出股數 = 'dealer_proprietary_sell_shares', (int, sql.dtype.int)
    自營商_避險_買進股數 = 'dealer_hedge_buy_shares', (int, sql.dtype.int)
    自營商_避險_賣出股數 = 'dealer_hedge_sell_shares', (int, sql.dtype.int)
    自營商買進股數 = 'dealer_buy_shares', (int, sql.dtype.int)
    自營商賣出股數 = 'dealer_sell_shares', (int, sql.dtype.int)

class Ownership(Field):
    __chinese__ = '持股比例'
    總發行股數 = 'total_outstanding_shares', (int, sql.dtype.int)
    外陸資持有股數 = 'foreign_holding_shares', (int, sql.dtype.int)
    外陸資投資上限比率 = 'foreign_investment_limit_ratio', (float, sql.dtype.float)

class ShareholdingDistribution(Field):
    __chinese__ = '股權分散表'
    零股_人數 =  'below_1_shareholders', (int, sql.dtype.int)
    一至五張_人數 = 'between_1_5_shareholders', (int, sql.dtype.int)
    五至十張_人數 = 'between_5_10_shareholders', (int, sql.dtype.int)
    十至十五張_人數 = 'between_10_15_shareholders', (int, sql.dtype.int)
    十五至二十張_人數 = 'between_15_20_shareholders', (int, sql.dtype.int)
    二十至三十張_人數 = 'between_20_30_shareholders', (int, sql.dtype.int)
    三十至四十張_人數 = 'between_30_40_shareholders', (int, sql.dtype.int)
    四十至五十張_人數 = 'between_40_50_shareholders', (int, sql.dtype.int)
    五十至一百張_人數 = 'between_50_100_shareholders', (int, sql.dtype.int)
    一百至兩百張_人數 = 'between_100_200_shareholders', (int, sql.dtype.int)
    兩百至四百張_人數 = 'between_200_400_shareholders', (int, sql.dtype.int)
    四百至六百張_人數 = 'between_400_600_shareholders', (int, sql.dtype.int)
    六百到八百張_人數 = 'between_600_800_shareholders', (int, sql.dtype.int)
    八百到一千張_人數 = 'between_800_1000_shareholders', (int, sql.dtype.int)
    千張以上_人數 = 'above_1000_shareholders', (int, sql.dtype.int)

    零股_股數 = 'below_1_sum_shares', (int, sql.dtype.int)
    一至五張_股數 = 'between_1_5_sum_shares', (int, sql.dtype.int)
    五至十張_股數 = 'between_5_10_sum_shares', (int, sql.dtype.int)
    十至十五張_股數 = 'between_10_15_sum_shares', (int, sql.dtype.int)
    十五至二十張_股數 = 'between_15_20_sum_shares', (int, sql.dtype.int)
    二十至三十張_股數 = 'between_20_30_sum_shares', (int, sql.dtype.int)
    三十至四十張_股數 = 'between_30_40_sum_shares', (int, sql.dtype.int)
    四十至五十張_股數 = 'between_40_50_sum_shares', (int, sql.dtype.int)
    五十至一百張_股數 = 'between_50_100_sum_shares', (int, sql.dtype.int)
    一百至兩百張_股數 = 'between_100_200_sum_shares', (int, sql.dtype.int)
    兩百至四百張_股數 = 'between_200_400_sum_shares', (int, sql.dtype.int)
    四百至六百張_股數 = 'between_400_600_sum_shares', (int, sql.dtype.int)
    六百到八百張_股數 = 'between_600_800_sum_shares', (int, sql.dtype.int)
    八百到一千張_股數 = 'between_800_1000_sum_shares', (int, sql.dtype.int)
    千張以上_股數 = 'above_1000_sum_shares', (int, sql.dtype.int)

class CompanyInfo(Field):
    __chinese__ = '公司資訊'
    代號 = 'id', (str, sql.dtype.str)
    名稱 = 'name', (str, sql.dtype.str)
    市場別 = 'market', (str, sql.dtype.str)
    產業別 = 'industry', (str, sql.dtype.str)
    題材 = 'theme', (str, sql.dtype.str)
