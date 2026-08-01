from ..base import TWSEScraper

class URL_0(TWSEScraper):
    MIN_SLEEP_TIME = 0
    MAX_SLEEP_TIME = 0.1

    def create_request_info(self, date):
        url = 'https://openapi.twse.com.tw/v1/opendata/t187ap14_L'
        return url
    
    def request(self, session, request_info, timeout):
        res = session.get(request_info, timeout=timeout)
        return res

url_0 = URL_0('D', '.json')