import json

from . import Checker

class TWSEChecker(Checker):
    
    def read(self, path):
        with open(path, encoding='utf-8') as f:
            content = json.load(f)
        return content
    
class STOCKS(TWSEChecker):

    def compare(self, c1, c2):
        drop_name = 'params'
        if (drop_name in c1) and (drop_name in c2):
            c1.pop(drop_name)
            c2.pop(drop_name)
        return c1 == c2

class Market(TWSEChecker):
    
    def compare(self, c1, c2):
        return c1 == c2

stocks = STOCKS()
market = Market()