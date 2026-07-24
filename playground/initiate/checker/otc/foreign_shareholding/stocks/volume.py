from ...base import OTCChecker
    
class VERSION_0(OTCChecker):
    def read(self, path):
        return path.read_text(encoding='utf-8')

version_0 = VERSION_0()