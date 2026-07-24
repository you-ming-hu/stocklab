from ...base import OTCScraper
    
class VERSION_0(OTCScraper):
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

version_0 = VERSION_0('D', '.json')

class VERSION_1(OTCScraper):
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

version_1 = VERSION_1('D', '.json')