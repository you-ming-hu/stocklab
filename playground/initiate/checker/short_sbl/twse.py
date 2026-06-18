import json

from .. import Checker

class TWSEChecker(Checker):
    
    def read(self, path):
        with open(path, encoding='utf-8') as f:
            content = json.load(f)
        return content
    
class STOCKS(TWSEChecker):

    def standardize(self, c):
        return c

stocks = STOCKS()