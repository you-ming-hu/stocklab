import pandas as pd
import pathlib
import json

from ...base import OTCScraper

class VERSION_0(OTCScraper):

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

version_0 = VERSION_0('D', '.html')

class VERSION_1(OTCScraper):

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
    
version_1 = VERSION_1('D', '.html')

class VERSION_2(VERSION_1):
    
    def create_request_info(self, date):
        url = 'https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes'
        data = dict(
            date = self.create_request_date(date),
            id = '',
            response = 'json'
        )
        return url, data

    def save(self, res, filename):
        data = res.json()
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

version_2 = VERSION_2('D', '.json')