import json

from .. import Checker

class TWSEChecker(Checker):
    
    def read(self, path):
        with open(path, encoding='utf-8') as f:
            content = json.load(f)
        return content
    
class STOCKS(TWSEChecker):

    def standardize(self, c):
        drop_name = 'params'
        if drop_name in c:
            c.pop(drop_name)
        return c

class MARKET(TWSEChecker):
    pass

stocks = STOCKS()
market = MARKET()