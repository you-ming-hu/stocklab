from ..utils import Field as F
from ..utils import FieldGroup as FG
from ..utils import disabled

class FlowVolume(FG):
    自營商_自行買賣_買進_股數 = F('dealer_proprietary_buy_volume', int)
    自營商_自行買賣_賣出_股數 = F('dealer_proprietary_sell_volume', int)
    
    自營商_避險_買進_股數 = F('dealer_hedge_buy_volume', int)
    自營商_避險_賣出_股數 = F('dealer_hedge_sell_volume', int)
    
    自營商_買進_股數 = F('dealer_buy_volume', int)
    自營商_賣出_股數 = F('dealer_sell_volume', int)

class FlowValue(FG):
    自營商_自行買賣_買進_金額 = F('dealer_proprietary_buy_value', int)
    自營商_自行買賣_賣出_金額 = F('dealer_proprietary_sell_value', int)
    
    自營商_避險_買進_金額 = F('dealer_hedge_buy_value', int)
    自營商_避險_賣出_金額 = F('dealer_hedge_sell_value', int)
    
    自營商_買進_金額 = F('dealer_buy_value', int)
    自營商_賣出_金額 = F('dealer_sell_value', int)

@disabled
class BalanceVolume(FG):
    自營商_自行買賣_餘額_股數 = F('dealer_proprietary_balance_volume', int)
    自營商_避險_餘額_股數 = F('dealer_hedge_balance_volume', int)
    自營商_餘額_股數 = F('dealer_balance_volume', int)

@disabled
class BalanceValue(FG):
    自營商_自行買賣_餘額_金額 = F('dealer_proprietary_balance_value', int)
    自營商_避險_餘額_金額 = F('dealer_hedge_balance_value', int)
    自營商_餘額_金額 = F('dealer_balance_value', int)

    
