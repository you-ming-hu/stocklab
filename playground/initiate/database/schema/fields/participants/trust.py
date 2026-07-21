from ..utils import Field as F
from ..utils import FieldGroup as FG
from ..utils import disabled

class FlowVolume(FG):
    投信_買進_股數 = F('trust_buy_volume', int)
    投信_賣出_股數 = F('trust_sell_volume', int)

class FlowValue(FG):
    投信_買進_金額 = F('trust_buy_value', int)
    投信_賣出_金額 = F('trust_sell_value', int)

@disabled
class BalanceVolume(FG):
    投信_餘額_股數 = F('trust_balance_volume', int)

@disabled
class BalanceValue(FG):
    投信_餘額_金額 = F('trust_balance_value', int)