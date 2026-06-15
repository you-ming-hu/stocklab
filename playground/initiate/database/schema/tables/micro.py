from .. import fields
from . import base

class StockDaily(base.DataTimestampTable):
    __primary_keys__ = [
        fields.CompanyInfo.代號,
        fields.DataTimestamp.資料日期
    ]
    __additional_index__ = [
        fields.DataTimestamp.資料日期
    ]
    # 個股資訊
    f_stock_info = fields.StockInfo
    # 技術面
    f_techicals = fields.Technicals
    # 籌碼面 - 融資券
    f_margin = fields.Margin
    f_short = fields.Short
    f_short_additional = fields.ShortAdditional
    # 籌碼面 - 三大法人
    f_institution_share_flow = fields.InstitutionShareFlow
    # 籌碼面 - 持股比例
    f_ownership = fields.Ownership

class CompanyInfo(base.UpdateTimestampTable):
    __primary_keys__ = [
        fields.UpdateTimestamp.更新日期,
        fields.CompanyInfo.產業別,
        fields.CompanyInfo.題材,
        fields.CompanyInfo.市場別,
        fields.CompanyInfo.代號
    ]
    __additional_index__ = [
        fields.CompanyInfo.名稱
    ]
    f_company_info = fields.CompanyInfo

class ShareholdingDistribution(base.DataTimestampTable):
    __primary_keys__ = [
        fields.CompanyInfo.代號,
        fields.DataTimestamp.資料日期
    ]
    __additional_index__ = [
    ]
    f_shareholding_distribution = fields.ShareholdingDistribution

class FinancialStatement(base.DataTimestampTable):
    pass