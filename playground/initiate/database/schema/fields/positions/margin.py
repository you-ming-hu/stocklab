from ..utils import Field as F
from ..utils import FieldGroup as FG

class FlowVolume(FG):
    融資_買進_股數 = F('margin_buy_volume', int)
    融資_賣出_股數 = F('margin_sell_volume', int)
    融資_現償_股數 = F('margin_cash_repayment_volume', int)
    
class FlowValue(FG):
    融資_買進_金額 = F('margin_buy_value', int)
    融資_賣出_金額 = F('margin_sell_value', int)
    融資_現償_金額 = F('margin_cash_repayment_value', int)
    
class BalanceVolume(FG):
    融資_餘額_股數 = F('margin_balance_volume', int)

class BalanceValue(FG):
    融資_餘額_金額 = F('margin_balance_value', int)

class Limit(FG):
    融資_次日限額_股數 = F('margin_next_day_limit_volume', int)