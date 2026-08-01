import pathlib
import pandas as pd

from ...base import OTCScraper

class URL_0(OTCScraper):
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

url_0 = URL_0('D', '.html')

class URL_1(OTCScraper):
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

url_1 = URL_1('D', '.json')