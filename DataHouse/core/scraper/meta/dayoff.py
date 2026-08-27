from .. import Scraper

import time

class URL_0(Scraper):

    def request(self, session, request_info, timeout, method='post'):
        return super().request(session, request_info, method, timeout)

    def create_session(self):
        header = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://hist.tpex.org.tw/"
        }
        return super().create_session(header)

    def create_request_date(self, date, is_taiwanese=False, sep=''):
        return super().create_request_date(date, is_taiwanese, sep)[:4]

    def create_request_info(self, date):
        url = 'https://www.tpex.org.tw/www/zh-tw/bulletin/tradingDate'
        params = {
            "response": "json",
            "date": self.create_request_date(date),
            "_": ''
        }
        return url, params

url_0 = URL_0('YS', '.json')

class URL_1(Scraper):

    def request(self, session, request_info, timeout, method='get'):
        return super().request(session, request_info, method, timeout)

    def create_session(self):
        header = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.twse.com.tw/"
        }
        return super().create_session(header)

    def create_request_date(self, date, is_taiwanese=False, sep=''):
        return super().create_request_date(date, is_taiwanese, sep)
    
    def create_cache_id(self):
        return str(int(time.time()*1000))

    def create_request_info(self, date):
        url = 'https://www.twse.com.tw/rwd/zh/holidaySchedule/holidaySchedule'
        params = {
            "response": "json",
            "date": self.create_request_date(date),
            "_": self.create_cache_id()
        }
        return url, params

url_1 = URL_1('YS', '.json')