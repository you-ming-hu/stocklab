from ...base import TWSEScraper

class URL_0(TWSEScraper):
    
    def create_request_info(self, date):
        url = 'https://www.twse.com.tw/rwd/zh/fund/MI_QFIIS'
        params = {
            "response": "json",
            "selectType":"ALLBUT0999",
            "date": self.create_request_date(date),
            "_": self.create_cache_id()
        }
        return url, params

url_0 = URL_0('D', '.json')