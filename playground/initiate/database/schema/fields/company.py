from .utils import Field as F
from .utils import FieldGroup as FG

class Info(FG):
    代號 = F('id', str)
    名稱 = F('name', str)
    市場別 = F('market', str)
    主要登記產業 = F('main_industry', str)
    營運產業 = F('involved_industry', str)
    題材 = F('theme', str)