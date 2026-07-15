import datetime
from .utils import Field as F
from .utils import FieldGroup as FG

class DataTimestamp(FG):
    資料日期 = F('date', datetime.date)

class StockInfo(FG):
    代號 = F('id', str)
    名稱 = F('name', str)
    市場別 = F('market', str)
    交易中 = F('active', bool, False)

class CompanyInfo(FG):
    代號 = F('id', str)
    名稱 = F('name', str)
    市場別 = F('market', str)
    產業別 = F('industry', str)
    題材 = F('theme', str)