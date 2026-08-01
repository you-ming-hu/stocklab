from ..base import OTCScraper

class URL_0(OTCScraper):
    MIN_SLEEP_TIME = 0
    MAX_SLEEP_TIME = 0.1
    
    def create_request_info(self, date):
        url = 'https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap14_O'
        return url
    
    def request(self, session, request_info, timeout):
        res = session.get(request_info, timeout=timeout)
        return res

url_0 = URL_0('D', '.json')