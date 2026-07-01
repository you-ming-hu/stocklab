from ..base import TWSEChecker

class MARKET(TWSEChecker):
    def standardize(self, c):
        drop_name = 'params'
        if drop_name in c:
            c.pop(drop_name)
        return c

market = MARKET()