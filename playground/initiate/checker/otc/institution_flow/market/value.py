import re

from ...base import OTCChecker

class VERSION_0(OTCChecker):
    
    def read(self, path):
        return path.read_text(encoding='utf-8')
    
    def standardize(self, c):
        c = re.sub(r'<script>.*?</script>', '', c)
        return c

version_0 = VERSION_0()

class VERSION_1(OTCChecker):
    pass

version_1 = VERSION_1()