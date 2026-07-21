from ...base import OTCScraper
    
class VERSION_0(OTCScraper):

    def create_request_info(self, date):
        url = 'https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingIndex'
        data = dict(
            date = self.create_request_date(date),
            id = '',
            response = 'json'
        )
        return url, data

version_0 = VERSION_0('MS', '.json')