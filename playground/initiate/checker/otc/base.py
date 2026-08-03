from .. import Checker

class OTCChecker(Checker):
    
    def old_api_read(self, path):
        return path.read_text(encoding='utf-8')