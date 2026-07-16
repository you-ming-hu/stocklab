import datetime
from .utils import Field as F
from .utils import FieldGroup as FG

class DataTimestamp(FG):
    資料日期 = F('date', datetime.date)