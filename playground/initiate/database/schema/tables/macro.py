from .. import fields
from . import base

class MarketDaily(base.DataTimestampTable):
    # 技術面
    f_techicals = fields.Technicals
    # 籌碼面 - 融資券
    f_margin = fields.Margin
    f_margin_additional = fields.MarginAdditional
    f_short = fields.Short
    # 籌碼面 - 三大法人
    f_institution_share_flow = fields.InstitutionShareFlow
    f_institution_fund_flow = fields.InstitutionFundFlow
    
class TWSEDaily(MarketDaily):
    __primary_keys__ = [
        fields.DataTimestamp.資料日期
    ]
    __additional_index__ = [
        
    ]

class OTCDaily(MarketDaily):
    __primary_keys__ = [
        fields.DataTimestamp.資料日期
    ]
    __additional_index__ = [
        
    ]

