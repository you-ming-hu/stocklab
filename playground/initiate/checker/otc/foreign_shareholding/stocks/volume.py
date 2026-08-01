from ...base import OTCChecker
    
class URL_0(OTCChecker):
    def read(self, path):
        return path.read_text(encoding='utf-8')

url_0 = URL_0()