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
    f_stock_info = fields.StockInfo
    f_techicals = fields.Technicals

class CompanyInfo(base.UpdateTimestampTable):
    __primary_keys__ = [
        fields.UpdateTimestamp.更新日期,
        fields.CompanyInfo.產業別,
        fields.CompanyInfo.題材,
        fields.CompanyInfo.市場別,
        fields.CompanyInfo.代號
    ]
    __additional_index__ = [
        
    ]
    f_company_info = fields.CompanyInfo

class FinancialStatement(base.DataTimestampTable):
    pass