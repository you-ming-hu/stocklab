from ...base import TWSEScraper
    
class VERSION_0(TWSEScraper):

    def create_request_info(self, date):
        url = 'https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK'
        params = {
            "response": "json",
            "date": self.create_request_date(date),
            "_": self.create_cache_id()
        }
        return url, params

version_0 = VERSION_0('MS', '.json')