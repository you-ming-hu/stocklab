from .utils import Field as F
from .utils import FieldGroup as FG

class DayOff(FG):
    原因 = F('reason', str)

class Season(FG):
    年度 = F('year', int)
    季別 = F('season', int)
