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

class MARKET_VOLUME(TWSEChecker):
    pass

class MARKET_PRICE(TWSEChecker):
    pass

stocks = STOCKS()
market_volume = MARKET_VOLUME()
market_price = MARKET_PRICE()