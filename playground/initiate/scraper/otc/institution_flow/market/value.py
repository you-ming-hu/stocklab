import pathlib
import pandas as pd

from ...base import OTCScraper

class URL_0(OTCScraper):
    def create_request_info(self, date):
        root_url = 'https://hist.tpex.org.tw/Hist/STOCK/3INSTI/3INSTI_SUMMARY'
        date = self.create_request_date(date, is_taiwanese=True, sep='')
        url = '/'.join([root_url, f'BIGDSUM{date}.htm'])
        return url
    
    def request(self, session, request_info, timeout):
        return self.old_api_request(session, request_info, timeout)

    def save(self, res, filename):
        return self.old_api_save(res, filename)
        
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