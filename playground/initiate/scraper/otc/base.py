from .. import Scraper

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