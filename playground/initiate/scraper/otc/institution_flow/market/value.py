import pathlib
import pandas as pd

from ...base import OTCScraper

class VERSION_0(OTCScraper):
    def create_request_info(self, date):
        root_url = 'https://hist.tpex.org.tw/Hist/STOCK/3INSTI/3INSTI_SUMMARY/BIGDSUM'
        date = self.create_request_date(date, is_taiwanese=True, sep='')
        url = root_url + date + '.htm'
        return url
    
    def request(self, session, request_info, timeout):
        res = session.get(request_info, timeout=timeout)
        return res

    def save(self, res, filename):
        res.encoding = 'big5'
        pathlib.Path(filename).write_text(res.text, encoding='utf-8')

version_0 = VERSION_0('D', '.html')

class VERSION_1(OTCScraper):
    def create_request_info(self, date):
        if pd.Timestamp(date) <= pd.Timestamp('2016/12/30'):
            prod = '0'
        else:
            prod = '1'
        url = 'https://www.tpex.org.tw/www/zh-tw/insti/summary'
        data = dict(
            type = 'Daily',
            prod = prod,
            date = self.create_request_date(date),
            id = '',
            response = 'json'
        )
        return url, data

version_1 = VERSION_1('D', '.json')