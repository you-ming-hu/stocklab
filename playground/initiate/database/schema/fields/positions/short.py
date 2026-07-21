from ..utils import Field as F
from ..utils import FieldGroup as FG
from ..utils import disabled

class FlowVolume(FG):
    融券_賣出_股數 = F('short_sell_volume', int)
    融券_買進_股數 = F('short_cover_volume', int)
    融券_現償_股數 = F('short_stock_repayment_volume', int)
    
class FlowValue(FG):
    融券_賣出_金額 = F('short_sell_value', int)

class BalanceVolume(FG):
    融券_餘額_股數 = F('short_balance_volume', int)

@disabled
class BalanceValue(FG):
    融券_餘額_金額 = F('short_balance_value', int)

class Limit(FG):
    融券_次日限額_股數 = F('short_next_day_limit_volume', int)
    