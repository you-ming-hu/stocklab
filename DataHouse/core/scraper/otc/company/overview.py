from ... import CompanyInfoScraper
from ..base import OTCScraper

class URL_0(OTCScraper, CompanyInfoScraper):
    MIN_SLEEP_TIME = 0
    MAX_SLEEP_TIME = 0.1

    def create_overview_session(self):
        return self.create_session()
    
    def create_overview_request_info(self):
        url = 'https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap14_O'
        return url

    def overview_request(self, session, request_info, timeout):
        res = session.get(request_info, timeout=timeout)
        return res

    def parse_company_table(self, table):
        mapping = {
            'SecuritiesCompanyCode': '代號',
            'CompanyName': '名稱',
            '產業別': '主要登記產業',
        }
        df = super().parse_company_table(table, mapping)
        return df

url_0 = URL_0()

