from .base import MetaTable
from .. import fields

class Table(metaclass = MetaTable):
    f_general = fields.General
    
class Stocks(Table):
    __primary_keys__ = [
        fields.General.資料日期,
        fields.CompanyInfo.代號
    ]
    __additional_index__ = [
        fields.General.資料日期
    ]
    f_company_info = fields.CompanyInfo
    f_techicals = fields.Technicals


class Market(Table):
    __primary_keys__ = [
        fields.General.資料日期
    ]
    __additional_index__ = [
        
    ]
    f_techicals = fields.Technicals
    
class OTC(Market):
    pass

class TWSE(Market):
    pass