from ..... import schema

from ..base import SUM

from ..stocks import foreign_balance_volume as base

class VERSION_0(SUM, base.VERSION_0):
    pass
    
version_0 = VERSION_0(
    schema.tables.OTCDaily,
    {
        '發行股數': schema.tables.OTCDaily.f_market_info.總發行股數,
        '全體外資持有股數': schema.tables.OTCDaily.f_foreign_balance_volume.外陸資_餘額_股數,
    },
    True
)

class VERSION_1(SUM, base.VERSION_1):
    pass

version_1 = VERSION_1(
    schema.tables.OTCDaily,
    {
        '發行股數': schema.tables.OTCDaily.f_market_info.總發行股數,
        '全體外資及陸資持有股數': schema.tables.OTCDaily.f_foreign_balance_volume.外陸資_餘額_股數,
    },
    True
)