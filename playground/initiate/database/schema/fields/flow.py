from .utils import Field as F
from .utils import FieldGroup as FG

class ForeignFlowShares(FG):
    外陸資_不含外資自營商_買進股數 = F('foreign_ex_dealer_buy_shares', int)
    外陸資_不含外資自營商_賣出股數 = F('foreign_ex_dealer_sell_shares', int)
    外資自營商買進股數 = F('foreign_dealer_buy_shares', int)
    外資自營商賣出股數 = F('foreign_dealer_sell_shares', int)
    外陸資買進股數 = F('foreign_buy_shares', int)
    外陸資賣出股數 = F('foreign_sell_shares', int)

class ForeignFlowAmount(FG):
    外陸資_不含外資自營商_買進金額 = F('foreign_ex_dealer_buy_amount', int)
    外陸資_不含外資自營商_賣出金額 = F('foreign_ex_dealer_sell_amount', int)
    外資自營商買進金額 = F('foreign_dealer_buy_amount', int)
    外資自營商賣出金額 = F('foreign_dealer_sell_amount', int)
    外陸資買進金額 = F('foreign_buy_amount', int)
    外陸資賣出金額 = F('foreign_sell_amount', int)

class TrustFlowShares(FG):
    投信買進股數 = F('trust_buy_shares', int)
    投信賣出股數 = F('trust_sell_shares', int)

class TrustFlowAmount(FG):
    投信買進金額 = F('trust_buy_amount', int)
    投信賣出金額 = F('trust_sell_amount', int)

class DealerFlowShares(FG):
    自營商_自行買賣_買進股數 = F('dealer_proprietary_buy_shares', int)
    自營商_自行買賣_賣出股數 = F('dealer_proprietary_sell_shares', int)
    自營商_避險_買進股數 = F('dealer_hedge_buy_shares', int)
    自營商_避險_賣出股數 = F('dealer_hedge_sell_shares', int)
    自營商買進股數 = F('dealer_buy_shares', int)
    自營商賣出股數 = F('dealer_sell_shares', int)

class DealerFlowAmount(FG):
    自營商_自行買賣_買進金額 = F('dealer_proprietary_buy_amount', int)
    自營商_自行買賣_賣出金額 = F('dealer_proprietary_sell_amount', int)
    自營商_避險_買進金額 = F('dealer_hedge_buy_amount', int)
    自營商_避險_賣出金額 = F('dealer_hedge_sell_amount', int)
    自營商買進金額 = F('dealer_buy_amount', int)
    自營商賣出金額 = F('dealer_sell_amount', int)