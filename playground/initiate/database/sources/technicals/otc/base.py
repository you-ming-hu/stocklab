from ...base import OTCSource
from ..base import STOCKS

class OTC_STOCKS(STOCKS, OTCSource):
    market_type = 'OTC'