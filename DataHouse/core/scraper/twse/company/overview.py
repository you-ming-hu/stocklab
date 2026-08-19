from ... import CompanyInfoScraper
from ..base import TWSEScraper

class URL_0(TWSEScraper, CompanyInfoScraper):
    MIN_SLEEP_TIME = 0
    MAX_SLEEP_TIME = 0.1

    def create_overview_session(self):
        return self.create_session()
    
    def create_overview_request_info(self):
        url = 'https://openapi.twse.com.tw/v1/opendata/t187ap14_L'
        return url

    def overview_request(self, session, request_info, timeout):
        res = session.get(request_info, timeout=timeout)
        return res

    def parse_company_table(self, table):
        mapping = {
            '公司代號': '代號',
            '公司名稱': '名稱',
            '產業別': '主要登記產業',
        }
        df = super().parse_company_table(table, mapping)
        return df

url_0 = URL_0()