from . import Checker

class STOCKS(Checker):
    
    def read(self, path):
        return path.read_text(encoding='utf-8')

class STOCKS_STAGE_1(STOCKS):
    
    def standardize(self, c):
        # standard process but extremely slow
        # from bs4 import BeautifulSoup
        # c = BeautifulSoup(c, "html.parser")
        # c.find("script").decompose()
        # return c.text
        return c.split('\n')[:-5]

class STOCKS_STAGE_2(STOCKS):
    pass

class STOCKS_STAGE_3(STOCKS):
    pass
    
stocks_stage_1 = STOCKS_STAGE_1()
stocks_stage_2 = STOCKS_STAGE_2()
stocks_stage_3 = STOCKS_STAGE_3()