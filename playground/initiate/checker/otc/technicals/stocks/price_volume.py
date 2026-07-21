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

class VERSION_0(PRICE_VOLUME):
    pass
    
version_0 = VERSION_0()

class VERSION_1(PRICE_VOLUME):
    pass
    
version_1 = VERSION_1()

class VERSION_2(PRICE_VOLUME):
    pass
    
version_2 = VERSION_2()