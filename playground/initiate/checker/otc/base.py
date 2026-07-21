from .. import Checker

class OTCChecker(Checker):
    
    def read(self, path):
        return path.read_text(encoding='utf-8')
    
    def standardize(self, c):
        # standard process but extremely slow
        # from bs4 import BeautifulSoup
        # c = BeautifulSoup(c, "html.parser")
        # c.find("script").decompose()
        # return c.text
        return c.split('\n')[:-5]