from ..base import OTCScraper

class VERSION_0(OTCScraper):
    def create_request_info(self, date):
        url = 'https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap14_O'
        return url
    
    def request(self, session, request_info, timeout):
        res = session.get(request_info, timeout=timeout)
        return res

version_0 = VERSION_0('D', '.json')