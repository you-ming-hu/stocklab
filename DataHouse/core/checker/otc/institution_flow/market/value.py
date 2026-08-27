from ...base import OTCChecker

import re

class URL_0(OTCChecker):
    
    def read(self, path):
        return self.old_api_read(path)
        
    def standardize(self, c):
        c = re.sub(r'<script>.*?</script>', '', c)
        return c

url_0 = URL_0()

class URL_1(OTCChecker):
    pass

url_1 = URL_1()