from ..base import OVERVIEW_V0, OTC
from ....schema.tables import CompanyInfo

class VERSION_0(OTC,OVERVIEW_V0):
    pass

version_0 = VERSION_0(
    CompanyInfo,
    {
        'SecuritiesCompanyCode': CompanyInfo.f_company_info.代號,
        'CompanyName': CompanyInfo.f_company_info.名稱,
        '產業別': CompanyInfo.f_company_info.主要登記產業,
    },
    True
)
