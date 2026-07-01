import pandas as pd
import pathlib

from ..base import OTCScraper

class STOCKS_STAGE_2(OTCScraper):

    def create_request_date(self, date):
        if isinstance(date, pd.Timestamp):
            pass
        elif isinstance(date, str):
            date = pd.Timestamp(date)
        else:
            assert False, 'not recognized date type'
        date = date.strftime('%Y/%m/%d')
        return date
    
    def create_request_info(self, date):
        url = 'https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotesHis'
        data = dict(
            date = self.create_request_date(date),
            id = '',
            response = 'json'
        )
        return url, data
    
    def request(self, session, request_info, timeout):
        url, data = request_info
        res = session.post(url, data=data, timeout=timeout)
        return res

    def save(self, res, filename):
        pathlib.Path(filename).write_text(res.json().get('html',''), encoding='utf-8-sig')
    
stocks_stage_2 = STOCKS_STAGE_2('D', '.html')