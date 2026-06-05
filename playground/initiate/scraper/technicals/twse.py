import pandas as pd
import time
import json

from .. import Scraper

class TWSEScraper(Scraper):

    def request(self, session, request_info, timeout):
        url = request_info
        res = session.get(url, timeout=timeout)
        return res

    def save(self, res, filename):
        data = res.json()
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def create_session(self):
        header = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.twse.com.tw/"
        }
        return super().create_session(header)

    def create_request_date(self, date):
        if isinstance(date, pd.Timestamp):
            date = date.strftime("%Y%m%d")
        elif isinstance(date, str):
            date = pd.Timestamp(date)
            date = date.strftime("%Y%m%d")
        else:
            assert False, 'not recognized date type'
        date = f'date={date}'
        return date
    
    def create_cache_id(self):
        return f'_={int(time.time()*1000)}'
    
    def assemble_request_url(self, url, *contents):
        return url + '?' + '&'.join(contents)

class STOCKS(TWSEScraper):

    def create_request_info(self, date):
        root_url = 'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX' # 每日收盤行情(全部(不含權證、牛熊證、可展延牛熊證))
        date = self.create_request_date(date)
        category = 'type=ALLBUT0999'
        format = 'response=json'
        cache_id = self.create_cache_id()
        url = self.assemble_request_url(root_url, date, category, format, cache_id)
        return url
    
class MARKET(TWSEScraper):

    def create_request_info(self, date):
        root_url = 'https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK'
        date = self.create_request_date(date)
        format = 'response=json'
        cache_id = self.create_cache_id()
        url = self.assemble_request_url(root_url, date, format, cache_id)
        return url

stocks = STOCKS('D', '.json')
market = MARKET('MS', '.json')