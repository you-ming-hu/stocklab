from .. import Scraper

import pandas as pd
import time

class TWSEScraper(Scraper):

    def request(self, session, request_info, timeout):
        return super().request(session, request_info, 'get', timeout)

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