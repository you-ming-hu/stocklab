import re

from ...base import OTCChecker

class URL_0(OTCChecker):
    
    def read(self, path):
        return path.read_text(encoding='utf-8')
    
    def standardize(self, c):
        c = re.sub(r'<script>.*?</script>', '', c)
        return c

url_0 = URL_0()

class URL_1(OTCChecker):
    pass

url_1 = URL_1()