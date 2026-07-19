from .utils import Field as F
from .utils import FieldGroup as FG

class Info(FG):
    代號 = F('id', str)
    名稱 = F('name', str)
    市場別 = F('market', str)
    產業別 = F('industry', str)
    題材 = F('theme', str)