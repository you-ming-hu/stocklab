
from ..base import TWSEScraper
    
class MARKET_PRICE(TWSEScraper):

    def create_request_info(self, date):
        url = 'https://www.twse.com.tw/indicesReport/MI_5MINS_HIST'
        params = {
            "response": "json",
            "date": self.create_request_date(date),
            "_": self.create_cache_id()
        }
        return url, params

market_price = MARKET_PRICE('MS', '.json')