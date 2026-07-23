from ...base import OTCScraper

class VERSION_0(OTCScraper):

    def create_request_info(self, date):
        url = 'https://www.tpex.org.tw/www/zh-tw/margin/sblHis'
        data = dict(
            date = self.create_request_date(date),
            id = '',
            response = 'json'
        )
        return url, data

version_0 = VERSION_0('D', '.json')

class VERSION_1(OTCScraper):

    def create_request_info(self, date):
        url = 'https://www.tpex.org.tw/www/zh-tw/margin/sblHis2'
        data = dict(
            date = self.create_request_date(date),
            id = '',
            response = 'json'
        )
        return url, data

version_1 = VERSION_1('D', '.json')

class VERSION_2(OTCScraper):

    def create_request_info(self, date):
        url = 'https://www.tpex.org.tw/www/zh-tw/margin/sbl'
        data = dict(
            date = self.create_request_date(date),
            id = '',
            response = 'json'
        )
        return url, data

version_2 = VERSION_2('D', '.json')