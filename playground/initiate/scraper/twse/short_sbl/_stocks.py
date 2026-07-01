from ..base import TWSEScraper

class STOCKS(TWSEScraper):

    def create_request_info(self, date):
        url = 'https://www.twse.com.tw/exchangeReport/TWT93U'
        params = {
            "response": "json",
            "date": self.create_request_date(date),
            "_": self.create_cache_id()
        }
        return url, params

stocks = STOCKS('D', '.json')