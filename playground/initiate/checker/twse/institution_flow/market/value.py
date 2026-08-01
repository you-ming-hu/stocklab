from ...base import TWSEChecker

class URL_0(TWSEChecker):
    def standardize(self, c):
        drop_name = 'params'
        if drop_name in c:
            c.pop(drop_name)
        return c

url_0 = URL_0()