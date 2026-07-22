from ..base import TWSEScraper

class VERSION_0(TWSEScraper):
    def create_request_info(self, date):
        url = 'https://openapi.twse.com.tw/v1/opendata/t187ap14_L'
        return url
    
    def request(self, session, request_info, timeout):
        res = session.get(request_info, timeout=timeout)
        return res

version_0 = VERSION_0('D', '.json')