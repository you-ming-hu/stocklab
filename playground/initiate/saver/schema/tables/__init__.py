from .base import Table
from .. import fields
    
class Stocks(metaclass = Table):
    __primary_keys__ = [
        fields.General.日期,
        fields.CompanyInfo.代號
    ]
    __additional_index__ = [
        fields.General.日期
    ]

    f_general = fields.General
    f_company_info = fields.CompanyInfo
    f_techicals = fields.Technicals
    f_sharehold = fields.Sharehold

class Market(metaclass = Table):
    __primary_keys__ = [
        fields.General.日期
    ]
    __additional_index__ = [
        
    ]
    f_general = fields.General
    f_techicals = fields.Technicals
    
class OTC(Market):
    pass

class TWSE(Market):
    pass