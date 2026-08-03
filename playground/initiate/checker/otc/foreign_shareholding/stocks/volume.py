from ...base import OTCChecker
    
class URL_0(OTCChecker):
    
    def read(self, path):
        return self.old_api_read(path)
        
url_0 = URL_0()