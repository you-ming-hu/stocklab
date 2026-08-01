from .. import Scraper

import pathlib

class OTCScraper(Scraper):

    def request(self, session, request_info, timeout, method='post'):
        return super().request(session, request_info, method, timeout)
    
    def create_session(self):
        header = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://hist.tpex.org.tw/"
        }
        return super().create_session(header)
    
    def create_request_date(self, date, is_taiwanese=False, sep='/'):
        return super().create_request_date(self, date, is_taiwanese, sep)
    
    def old_api_request(self, session, request_info, timeout):
        res = session.get(request_info, timeout=timeout)
        return res

    def old_api_save(self, res, filename):
        res.encoding = 'big5'
        pathlib.Path(filename).write_text(res.text, encoding='utf-8')
