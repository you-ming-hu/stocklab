from ...base import Source
from ..base import STOCKS

class OTC_STOCKS_V0(STOCKS, Source):
    market_type = 'OTC'

    def open(self, file):
        return super().open(file, 'text', True)

class OTC_STOCKS_V1(STOCKS, Source):
    market_type = 'OTC'

class OTC_MARKET_V0(Source):

    def to_df(self, content):
        table = content['tables'][0]
        df = super().to_df(table)
        return df

    def check_empty(self, content):
        return content['stat'] != 'ok'
        