from ...base import TWSEChecker
    
class VERSION_0(TWSEChecker):
    def standardize(self, c):
        data_name = 'data'
        if data_name in c:
            c[data_name] = {v[0]:v for v in c[data_name]}
        return c

version_0 = VERSION_0()