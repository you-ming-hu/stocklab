from .. import Scraper

import pandas as pd

class OTCScraper(Scraper):
    
    def create_session(self):
        header = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://hist.tpex.org.tw/"
        }
        return super().create_session(header)
    
    def create_request_date(self, date, is_taiwanese=False, sep='/'):
        date = pd.Timestamp(date)
        if not is_taiwanese:
            date = date.strftime(sep.join(['%Y','%m','%d']))
        else:
            date = sep.join([
                f'{date.year-1911}',
                f'{date.month:0>2}',
                f'{date.day:0>2}'
            ])
        return date
    
    def request(self, session, request_info, timeout):
        return super().request(session, request_info, 'post', timeout)