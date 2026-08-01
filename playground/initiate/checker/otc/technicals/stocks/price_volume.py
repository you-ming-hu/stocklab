from ...base import OTCChecker

class PRICE_VOLUME(OTCChecker):
    
    def read(self, path):
        return path.read_text(encoding='utf-8')
    
    def standardize(self, c):
        # standard process but extremely slow
        # from bs4 import BeautifulSoup
        # c = BeautifulSoup(c, "html.parser")
        # c.find("script").decompose()
        # return c.text
        return c.split('\n')[:-5]

class URL_0(PRICE_VOLUME):
    pass
    
url_0 = URL_0()

class URL_1(PRICE_VOLUME):
    pass
    
url_1 = URL_1()

class URL_2(PRICE_VOLUME):
    pass
    
url_2 = URL_2()