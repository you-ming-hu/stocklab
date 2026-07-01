from ..base import TWSEScraper

class ALL(TWSEScraper):

    def create_request_info(self, date):
        url = 'https://www.twse.com.tw/exchangeReport/MI_MARGN'
        params = {
            "response": "json",
            "selectType": 'ALL',
            "date": self.create_request_date(date),
            "_": self.create_cache_id()
        }
        return url, params

all = ALL('D', '.json')