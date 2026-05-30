from . import Checker

class STOCKS(Checker):
    
    def read(self, path):
        return path.read_text(encoding='utf-8')

class STOCKS_STAGE_1(STOCKS):
    
    def compare(self, c1, c2):
        drop_name = 'params'
        if (drop_name in c1) and (drop_name in c2):
            c1.pop(drop_name)
            c2.pop(drop_name)
        return c1 == c2
    
stocks_stage_1 = STOCKS_STAGE_1()

