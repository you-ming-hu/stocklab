from ..base import OVERVIEW_V0, OTC
from ....schema.tables import CompanyInfo

class VERSION_0(OTC,OVERVIEW_V0):
    pass

version_0 = VERSION_0(
    CompanyInfo,
    {
        '代號': CompanyInfo.f_company_info.代號,
        '名稱': CompanyInfo.f_company_info.名稱,
        '主要登記產業': CompanyInfo.f_company_info.主要登記產業,
        '營運產業': CompanyInfo.f_company_info.營運產業,
        '題材': CompanyInfo.f_company_info.題材
    },
    True
)