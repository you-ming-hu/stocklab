from ...base import OTCScraper
    
class URL_0(OTCScraper):
    def create_request_info(self, date):
        url = 'https://www.tpex.org.tw/www/zh-tw/insti/dailyTradeHis'
        data = dict(
            type = 'Daily',
            cate = 'EW',
            date = self.create_request_date(date),
            id = '',
            response = 'json'
        )
        return url, data

url_0 = URL_0('D', '.json')

class URL_1(OTCScraper):
    def create_request_info(self, date):
        url = 'https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade'
        data = dict(
            type = 'Daily',
            cate = 'EW',
            date = self.create_request_date(date),
            id = '',
            response = 'json'
        )
        return url, data

url_1 = URL_1('D', '.json')