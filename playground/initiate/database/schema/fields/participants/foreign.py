from ..utils import Field as F
from ..utils import FieldGroup as FG
from ..utils import disabled

class FlowVolume(FG):
    外陸資_不含外資自營商_買進_股數 = F('foreign_ex_dealer_buy_volume', int)
    外陸資_不含外資自營商_賣出_股數 = F('foreign_ex_dealer_sell_volume', int)
    
    外資自營商_買進_股數 = F('foreign_dealer_buy_volume', int)
    外資自營商_賣出_股數 = F('foreign_dealer_sell_volume', int)
    
    外陸資_買進_股數 = F('foreign_buy_volume', int)
    外陸資_賣出_股數 = F('foreign_sell_volume', int)

class FlowValue(FG):
    外陸資_不含外資自營商_買進_金額 = F('foreign_ex_dealer_buy_value', int)
    外陸資_不含外資自營商_賣出_金額 = F('foreign_ex_dealer_sell_value', int)
    
    外資自營商_買進_金額 = F('foreign_dealer_buy_value', int)
    外資自營商_賣出_金額 = F('foreign_dealer_sell_value', int)
    
    外陸資_買進_金額 = F('foreign_buy_value', int)
    外陸資_賣出_金額 = F('foreign_sell_value', int)

class BalanceVolume(FG):
    外陸資_持有_股數 = F('foreign_balance_volume', int)

@disabled
class BalanceValue(FG):
    外陸資_持有_金額 = F('foreign_balance_value', int)

class Limit(FG):
    外陸資_投資上限_比率 = F('foreign_volume_limit_ratio', float)