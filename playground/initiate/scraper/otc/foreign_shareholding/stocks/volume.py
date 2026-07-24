import pathlib

from ...base import OTCScraper

class VERSION_0(OTCScraper):
    def create_request_info(self, date):
        url = 'https://mopsov.twse.com.tw/server-java/t13sa150_otc'
        y,m,d = self.create_request_date(date).split('/')
        params = dict(
            years = y,
            months = m,
            days = d,
            bcode = '',
            step = '2'
        )
        return url, params
    
    def save(self, res, filename):
        res.encoding = 'big5'
        pathlib.Path(filename).write_text(res.text, encoding='utf-8')

version_0 = VERSION_0('D', '.html')