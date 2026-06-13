from .. import fields
from . import base

class MarketDaily(base.DataTimestampTable):
    __primary_keys__ = [
        fields.DataTimestamp.資料日期
    ]
    __additional_index__ = [
        
    ]
    f_techicals = fields.Technicals
    
class TWSEDaily(MarketDaily):
    pass

class OTCDaily(MarketDaily):
    pass

