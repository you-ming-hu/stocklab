from .. import Scraper

class URL_1(Scraper):

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

url_1 = URL_1('WS', '.json')