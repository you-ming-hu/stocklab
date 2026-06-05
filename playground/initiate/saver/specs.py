import enum

class terms(enum.StrEnum):
    # general information
    代號 = 'id'
    名稱 = 'name'
    # technicals
    開盤價 = 'open'
    最高價 = 'high'
    最低價 = 'low'
    收盤價 = 'close'
    交易股數 = 'volume'
    交易金額 = 'turnover'
    交易筆數 = 'trades'
    # others...

class market_type(enum.StrEnum):
    twse = "TWSE"
    otc = "OTC"

import pydantic
import datetime

class Schema:
    def __init__(self, name, base=None):
        self.name = name
        self.base = base
    def create(self, *schema):
        return pydantic.create_model(
            self.name,
            __base__ = self.base,
            **{term: (dtype, term) for term, dtype in schema}
        )

BaseSchema = Schema('BaseSchema').create(
    ('date', datetime.date),
    (terms.開盤價, float),
    (terms.最高價, float),
    (terms.最低價, float),
    (terms.收盤價, float),
    (terms.交易股數, int),
    (terms.交易金額, int),
    (terms.交易筆數, int)
)

StocksSchema = Schema('StockSchema',BaseSchema).create(
    (terms.代號, str),
    (terms.名稱, str),
    ('market', market_type)
)

MarketSchema = Schema('MarketSchema',BaseSchema).create()

class table:
    base = BaseSchema()
    stocks = StocksSchema()
    market = MarketSchema()