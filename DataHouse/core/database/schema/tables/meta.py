from .. import fields
from . import base

class DayOff(base.Table):
    __primary_keys__ = [
        fields.base.DataTimestamp.資料日期
    ]
    __additional_index__ = []
    
    f_reason = fields.meta.DayOff