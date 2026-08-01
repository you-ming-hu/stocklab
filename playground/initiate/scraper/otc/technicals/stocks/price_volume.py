import pathlib

from ...base import OTCScraper

class URL_0(OTCScraper):
    
    def create_request_info(self, date):
        root_url = 'https://hist.tpex.org.tw/Hist/STOCK/AFTERTRADING/DAILY_CLOSE_QUOTES'
        date = self.create_request_date(date, is_taiwanese=True, sep='')
        url = '/'.join([root_url, f'RSTA3104_{date}.HTML'])
        return url
    
    def request(self, session, request_info, timeout):
        return self.old_api_request(session, request_info, timeout)

    def save(self, res, filename):
        return self.old_api_save(res, filename)

url_0 = URL_0('D', '.html')

class URL_1(OTCScraper):
    
    def create_request_info(self, date):
        url = 'https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotesHis'
        data = dict(
            date = self.create_request_date(date),
            id = '',
            response = 'json'
        )
        return url, data

    def save(self, res, filename):
        pathlib.Path(filename).write_text(res.json().get('html',''), encoding='utf-8-sig')
    
url_1 = URL_1('D', '.html')

class URL_2(OTCScraper):
    
    def create_request_info(self, date):
        url = 'https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes'
        data = dict(
            date = self.create_request_date(date),
            id = '',
            response = 'json'
        )
        return url, data

url_2 = URL_2('D', '.json')