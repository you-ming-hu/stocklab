import json

from .. import Checker

class TWSEChecker(Checker):
    
    def read(self, path):
        with open(path, encoding='utf-8') as f:
            content = json.load(f)
        return content
    
    def standardize(self, c):
        return c
    
class STOCKS(TWSEChecker):
    pass

class ETF(TWSEChecker):
    pass

class MARKET(TWSEChecker):
    pass

stocks = STOCKS()
etf = ETF()
market = MARKET()