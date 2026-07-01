import pandas as pd
import pathlib

from ..base import OTCScraper

class STOCKS_STAGE_1(OTCScraper):

    def create_request_date(self, date):
        if isinstance(date, pd.Timestamp):
            pass
        elif isinstance(date, str):
            date = pd.Timestamp(date)
        else:
            assert False, 'not recognized date type'
        date = f'{date.year-1911}{date.month:0>2}{date.day:0>2}'
        return date
    
    def create_request_info(self, date):
        root_url = 'https://hist.tpex.org.tw/Hist/STOCK/AFTERTRADING/DAILY_CLOSE_QUOTES'
        date = self.create_request_date(date)
        url = root_url + '/RSTA3104_' + date + '.HTML'
        return url
    
    def request(self, session, request_info, timeout):
        url = request_info
        res = session.get(url, timeout=timeout)
        return res

    def save(self, res, filename):
        res.encoding = 'big5'
        pathlib.Path(filename).write_text(res.text, encoding='utf-8')

stocks_stage_1 = STOCKS_STAGE_1('D', '.html')