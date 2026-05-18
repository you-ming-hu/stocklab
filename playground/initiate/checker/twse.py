import json

from . import Checker
    
class STOCKS(Checker):
    
    def equal(self, t1st, t2nd):
        try:
            read_path = t1st
            with open(read_path, encoding='utf-8') as f:
                file1st = json.load(f)
            
            read_path = t2nd
            with open(read_path, encoding='utf-8') as f:
                file2nd = json.load(f)
        except:
            print(f'error file: {read_path}')
            return False

        drop_name = 'params'
        if (drop_name in file1st) and (drop_name in file2nd):
            file1st.pop(drop_name)
            file2nd.pop(drop_name)
        return file1st == file2nd

stocks = STOCKS()
