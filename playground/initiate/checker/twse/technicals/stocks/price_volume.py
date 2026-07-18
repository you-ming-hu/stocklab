from ...base import TWSEChecker
    
class VERSION_0(TWSEChecker):

    def standardize(self, c):
        drop_name = 'params'
        if drop_name in c:
            c.pop(drop_name)
        return c

version_0 = VERSION_0()