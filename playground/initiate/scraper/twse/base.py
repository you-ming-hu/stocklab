from .. import Scraper

import time

class TWSEScraper(Scraper):

    def request(self, session, request_info, timeout, method='get'):
        return super().request(session, request_info, method, timeout)

    def create_session(self):
        header = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.twse.com.tw/"
        }
        return super().create_session(header)

    def create_request_date(self, date, is_taiwanese=False, sep=''):
        return super().create_request_date(self, date, is_taiwanese, sep)
    
    def create_cache_id(self):
        return str(int(time.time()*1000))