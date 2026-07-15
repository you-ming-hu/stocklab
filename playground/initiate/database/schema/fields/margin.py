from .utils import Field as F
from .utils import FieldGroup as FG

class MarginShares(FG):
    融資買進股數 = F('margin_buy_shares', int)
    融資賣出股數 = F('margin_sell_shares', int)
    融資現償股數 = F('margin_cash_repayment_shares', int)
    融資餘額股數 = F('margin_balance_shares', int)

class MarginAmount(FG):
    融資買進金額 = F('margin_buy_amount', int)
    融資賣出金額 = F('margin_sell_amount', int)
    融資現償金額 = F('margin_cash_repayment_amount', int)
    融資餘額金額 = F('margin_balance_amount', int)

class ShortShares(FG):
    融券買進股數 = F('short_cover_shares', int)
    融券賣出股數 = F('short_sell_shares', int)
    融券現償股數 = F('short_stock_repayment_shares', int)
    融券餘額股數 = F('short_balance_shares', int)

class ShortAmount(FG):
    融券成交金額 = F('short_transaction_amount', int)

class ShortLimit(FG):
    融券次日限額股數 = F('short_next_day_limit_shares', int)

class SLBShares(FG):
    借券賣出賣出股數 = F('slb_sell_shares', int)
    借券賣出還券股數 = F('slb_return_shares', int)
    借券賣出調整股數 = F('slb_adjustment_shares', int)
    借券賣出不含賣出總異動股數 = F('slb_change_ex_sell_shares', int)
    借券賣出餘額股數 = F('slb_balance_shares', int)

class SLBAmount(FG):
    借券賣出成交金額 = F('slb_transaction_amount', int)

class SLBLimit(FG):
    借券賣出次日限額股數 = F('slb_next_day_limit_shares', int)