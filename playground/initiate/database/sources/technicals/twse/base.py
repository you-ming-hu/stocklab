from ...base import TWSESource
from ..base import STOCKS

class TWSE_STOCKS(STOCKS, TWSESource):
    market_type = 'TWSE'

class TWSE_MARKET(TWSESource):
    def check_empty(self, content):
        return content['stat'] != 'OK'