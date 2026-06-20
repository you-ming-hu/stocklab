import pandas as pd
import time
import json

from .. import Scraper

class TWSEScraper(Scraper):

    def create_request_info(self, date, option):
        url = 'https://www.twse.com.tw/exchangeReport/MI_MARGN'
        params = {
            "response": "json",
            "selectType": option,
            "date": self.create_request_date(date),
            "_": self.create_cache_id()
        }
        return url, params

    def request(self, session, request_info, timeout):
        url, params = request_info
        res = session.get(url, params=params, timeout=timeout)
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
        return date
    
    def create_cache_id(self):
        return str(int(time.time()*1000))
    
class STOCKS(TWSEScraper):

    def create_request_info(self, date):
        return super().create_request_info(date, 'STOCK')

class ETF(TWSEScraper):

    def create_request_info(self, date):
        return super().create_request_info(date, '0099P')

class MARKET(TWSEScraper):
    def create_request_info(self, date):
        return super().create_request_info(date, 'ALL')

stocks = STOCKS('D', '.json')
etf = ETF('D', '.json')
market = MARKET('D', '.json')