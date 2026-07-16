from .. import fields
from . import base

class StockDaily(base.Table):
    __primary_keys__ = [
        fields.CompanyInfo.代號,
        fields.DataTimestamp.資料日期
    ]
    __additional_index__ = [
        fields.DataTimestamp.資料日期
    ]
    
    f_stock_info = fields.StockInfo
    
    f_techical_price = fields.Price
    f_technical_volume = fields.Volume
    
    f_margin_flow_volume = fields.positions.margin.FlowVolume
    f_margin_balance_volume = fields.positions.margin.BalanceVolume
    f_margin_limit = fields.positions.margin.Limit

    f_short_flow_volume = fields.positions.short.FlowVolume
    f_short_balance_volume = fields.positions.short.BalanceVolume
    f_short_flow_value = fields.positions.short.FlowValue
    f_short_limit = fields.positions.short.Limit
    
    f_sbl_flow_volume = fields.positions.shortSBL.FlowVolume
    f_sbl_balance_volume = fields.positions.shortSBL.BalanceVolume
    f_sbl_flow_value = fields.positions.shortSBL.FlowValue
    f_sbl_limit = fields.positions.shortSBL.Limit

    f_foreign_flow_volume = fields.participants.foreign.FlowVolume
    f_foreign_balance_volume = fields.participants.foreign.BalanceVolume
    f_foreign_limit = fields.participants.foreign.Limit

    f_trust_flow_volume = fields.participants.trust.FlowVolume

    f_dealer_flow_volume = fields.participants.dealer.FlowVolume

class CompanyInfo(base.Table):
    __primary_keys__ = [
        fields.DataTimestamp.資料日期,
        fields.CompanyInfo.產業別,
        fields.CompanyInfo.題材,
        fields.CompanyInfo.市場別,
        fields.CompanyInfo.代號
    ]
    __additional_index__ = [
        fields.CompanyInfo.名稱
    ]
    f_company_info = fields.CompanyInfo

class ShareholdingDistribution(base.Table):
    __primary_keys__ = [
        fields.CompanyInfo.代號,
        fields.DataTimestamp.資料日期
    ]
    __additional_index__ = [
    ]
    f_shareholding_distribution = fields.ShareholdingDistribution

class FinancialStatement(base.Table):
    pass