from ...base import Source
from ..base import STOCKS

class TWSE_STOCKS_V0(STOCKS, Source):
    market_type = 'TWSE'

class TWSE_MARKET_V0(Source):
    
    def check_empty(self, content):
        return content['stat'] != 'OK'