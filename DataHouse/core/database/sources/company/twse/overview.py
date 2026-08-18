from ..base import OVERVIEW_V0, TWSE
from ....schema.tables import CompanyInfo

class VERSION_0(TWSE,OVERVIEW_V0):
    pass

version_0 = VERSION_0(
    CompanyInfo,
    {
        '公司代號': CompanyInfo.f_company_info.代號,
        '公司名稱': CompanyInfo.f_company_info.名稱,
        '產業別': CompanyInfo.f_company_info.主要登記產業,
    },
    True
)
