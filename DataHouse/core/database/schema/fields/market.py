from .utils import Field as F
from .utils import FieldGroup as FG

class Info(FG):
    總發行股數 = F('total_outstanding_shares', int)