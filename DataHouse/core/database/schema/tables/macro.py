from .. import fields
from . import base

class MarketDaily(base.Table):

    f_market_info = fields.market.Info

    f_technicals_price = fields.technicals.Price
    f_technicals_volume = fields.technicals.Volume
    
    f_margin_flow_volume = fields.positions.margin.FlowVolume
    f_margin_balance_volume = fields.positions.margin.BalanceVolume
    f_margin_flow_value = fields.positions.margin.FlowValue
    f_margin_balance_value = fields.positions.margin.BalanceValue

    f_short_flow_volume = fields.positions.short.FlowVolume
    f_short_balance_volume = fields.positions.short.BalanceVolume
    f_short_flow_value = fields.positions.short.FlowValue
    
    f_sbl_flow_volume = fields.positions.shortSBL.FlowVolume
    f_sbl_balance_volume = fields.positions.shortSBL.BalanceVolume
    f_sbl_flow_value = fields.positions.shortSBL.FlowValue

    f_foreign_flow_volume = fields.participants.foreign.FlowVolume
    f_foreign_balance_volume = fields.participants.foreign.BalanceVolume
    f_foreign_flow_value = fields.participants.foreign.FlowValue

    f_trust_flow_volume = fields.participants.trust.FlowVolume
    f_trust_flow_value = fields.participants.trust.FlowValue

    f_dealer_flow_volume = fields.participants.dealer.FlowVolume
    f_dealer_flow_value = fields.participants.dealer.FlowValue
    
class TWSEDaily(MarketDaily):
    __primary_keys__ = [
        fields.base.DataTimestamp.資料日期
    ]
    __additional_index__ = [
        
    ]

class OTCDaily(MarketDaily):
    __primary_keys__ = [
        fields.base.DataTimestamp.資料日期
    ]
    __additional_index__ = [
        
    ]

