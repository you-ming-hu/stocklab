import json

from .. import Checker

class ALL(Checker):
    
    def read(self, path):
        with open(path, encoding='utf-8') as f:
            content = json.load(f)
        return content
    
    def standardize(self, c):
        return c

all = ALL()