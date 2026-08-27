from .. import Scraper

class URL_0(Scraper):
    MIN_SLEEP_TIME = 0
    MAX_SLEEP_TIME = 0.1

    def request(self, session, request_info, timeout, method='get'):
        return super().request(session, request_info, method, timeout)

    def create_session(self):
        header = {
            "User-Agent": "Mozilla/5.0",
        }
        return super().create_session(header)

    def create_request_info(self, date):
        url = 'https://openapi.tdcc.com.tw/v1/opendata/1-5'
        params = {}
        return url, params

url_0 = URL_0('W-FRI', '.json')