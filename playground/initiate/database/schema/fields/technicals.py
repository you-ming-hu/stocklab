from .utils import Field as F
from .utils import FieldGroup as FG

class TechnicalPrice(FG):
    開盤價 = F('open', float)
    最高價 = F('high', float)
    最低價 = F('low', float)
    收盤價 = F('close', float)

class TechnicalVolume(FG):
    交易股數 = F('volume', int)
    交易金額 = F('turnover', int)
    交易筆數 = F('trades', int)