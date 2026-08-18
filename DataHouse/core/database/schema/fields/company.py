from .utils import Field as F
from .utils import FieldGroup as FG

class Index(FG):
    代號 = F('id', str)

class Info(FG):
    代號 = F('id', str)
    名稱 = F('name', str)
    市場別 = F('market', str)
    主要登記產業 = F('main_industry', str)
    營運產業 = F('involved_industry', str)
    題材 = F('theme', str)

class FinancialStatement(FG):
    每股盈餘 = F('earnings_per_share', int)
    營業收入 = F('operating_revenue', int)
    營業利益 = F('operating_income', int)
    營業外收入及支出 = F('non_operating_income_expenses', int)
    稅後淨利 = F('net_income', int)