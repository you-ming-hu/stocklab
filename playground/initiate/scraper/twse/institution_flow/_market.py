from ..base import TWSEScraper

class MARKET(TWSEScraper):
    def create_request_info(self, date):
        url = 'https://www.twse.com.tw/rwd/zh/fund/BFI82U'
        params = {
            "response": "json",
            "type":"day",
            "dayDate": self.create_request_date(date),
            "_": self.create_cache_id()
        }
        return url, params

market = MARKET('D', '.json')