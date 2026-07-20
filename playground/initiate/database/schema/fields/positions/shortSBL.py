from ..utils import Field as F
from ..utils import FieldGroup as FG
from ..utils import disabled

class FlowVolume(FG):
    借券賣出_賣出_股數 = F('sbl_sell_volume', int)
    借券賣出_還券_股數 = F('sbl_return_volume', int)
    借券賣出_調整_股數 = F('sbl_adjustment_volume', int)
    借券賣出_不含賣出_總異動_股數 = F('sbl_ex_sell_change_volume', int)
    
class FlowValue(FG):
    借券賣出_賣出_金額 = F('sbl_transaction_value', int)

class BalanceVolume(FG):
    借券賣出_餘額_股數 = F('sbl_balance_volume', int)

@disabled
class BalanceValue(FG):
    借券賣出_餘額_金額 = F('sbl_balance_value', int)

class Limit(FG):
    借券賣出_次日限額_股數 = F('sbl_next_day_limit_volume', int)