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
    
    f_techical_price = fields.TechnicalPrice
    f_technical_volume = fields.TechnicalVolume
    
    f_margin_shares = fields.MarginShares
    f_short_shares = fields.ShortShares
    f_short_amount = fields.ShortAmount
    f_short_limit = fields.ShortLimit
    f_slb_shares = fields.SLBShares
    f_slb_amount = fields.SLBAmount
    f_slb_limit = fields.SLBLimit

    f_foreign_flow_shares = fields.ForeignFlowShares
    f_trust_flow_shares = fields.TrustFlowShares
    f_dealer_flow_shares = fields.DealerFlowShares
    
    f_outstanding_shares = fields.OutstandingShares
    f_foreign_shareholding = fields.ForeignShareholding

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