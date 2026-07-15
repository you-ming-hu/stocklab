from .. import fields
from . import base

class MarketDaily(base.Table):
    f_techical_price = fields.TechnicalPrice
    f_technical_volume = fields.TechnicalVolume
    
    f_margin_shares = fields.MarginShares
    f_margin_amount = fields.MarginAmount
    f_short_shares = fields.ShortShares
    f_short_amount = fields.ShortAmount
    f_slb_shares = fields.SLBShares
    f_slb_amount = fields.SLBAmount

    f_foreign_flow_shares = fields.ForeignFlowShares
    f_foreign_flow_amount = fields.ForeignFlowAmount
    f_trust_flow_shares = fields.TrustFlowShares
    f_trust_flow_amount = fields.TrustFlowAmount
    f_dealer_flow_shares = fields.DealerFlowShares
    f_dealer_flow_amount= fields.DealerFlowAmount
    
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

