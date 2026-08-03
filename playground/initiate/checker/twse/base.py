from .. import Checker

class TWSEChecker(Checker):
    
    def standardize(self, c, drop_name = 'params'):
        if drop_name in c:
            c.pop(drop_name)
        return c