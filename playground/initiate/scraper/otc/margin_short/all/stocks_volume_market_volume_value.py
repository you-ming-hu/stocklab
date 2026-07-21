import pathlib

from ...base import OTCScraper

class VERSION_0(OTCScraper):

    def create_request_info(self, date):
        root_url = 'https://hist.tpex.org.tw/Hist/STOCK/MARGIN_TRADING/MARGIN_BALANCE'
        date = self.create_request_date(date, is_taiwanese=True, sep='')
        url = root_url + '/RSTA3106_' + date + '.html'
        return url
    
    def request(self, session, request_info, timeout):
        url = request_info
        res = session.get(url, timeout=timeout)
        return res
    
    def save(self, res, filename):
        res.encoding = 'big5'
        pathlib.Path(filename).write_text(res.text, encoding='utf-8')

version_0 = VERSION_0('D', '.json')

class VERSION_1(OTCScraper):

    def create_request_info(self, date):
        url = 'https://www.tpex.org.tw/www/zh-tw/margin/balance'
        data = dict(
            date = self.create_request_date(date),
            id = '',
            response = 'json'
        )
        return url, data

version_1 = VERSION_1('D', '.json')
