from . import Checker

class STOCKS(Checker):
    
    def read(self, path):
        return path.read_text(encoding='utf-8')

class STOCKS_STAGE_1(STOCKS):
    
    def standardize(self, c):
        return c.split('\n')[:-3]

stocks_stage_1 = STOCKS_STAGE_1()

