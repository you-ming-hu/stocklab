from ...base import OTCScraper

class URL_0(OTCScraper):
    
    def create_request_info(self, date):
        url = 'https://www.tpex.org.tw/www/zh-tw/indexInfo/inx'
        data = dict(
            date = self.create_request_date(date),
            id = '',
            response = 'json'
        )
        return url, data

url_0 = URL_0('MS', '.json')