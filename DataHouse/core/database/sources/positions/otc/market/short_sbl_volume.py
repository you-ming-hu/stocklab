from .....schema.tables import OTCDaily

from ..base import SUM

from ..stocks import short_sbl_volume as base

class VERSION_0(SUM, base.VERSION_0):
    pass
    
version_0 = VERSION_0(
    OTCDaily,
    {
        '融券賣出': OTCDaily.f_short_flow_volume.融券_賣出_股數,
        '融券買進': OTCDaily.f_short_flow_volume.融券_買進_股數,
        '融券現券': OTCDaily.f_short_flow_volume.融券_現償_股數,
        '融券今日餘額': OTCDaily.f_short_balance_volume.融券_餘額_股數,
        '借券賣出': OTCDaily.f_sbl_flow_volume.借券賣出_賣出_股數,
        '借券庫存異動': OTCDaily.f_sbl_flow_volume.借券賣出_不含賣出_總異動_股數,
        '借券今日餘額': OTCDaily.f_sbl_balance_volume.借券賣出_餘額_股數,
    },
    True
)


class VERSION_1(SUM, base.VERSION_1):
    pass
    
version_1 = VERSION_1(
    OTCDaily,
    {
        '融券賣出': OTCDaily.f_short_flow_volume.融券_賣出_股數,
        '融券買進': OTCDaily.f_short_flow_volume.融券_買進_股數,
        '融券現券': OTCDaily.f_short_flow_volume.融券_現償_股數,
        '融券今日餘額': OTCDaily.f_short_balance_volume.融券_餘額_股數,
        '借券當日賣出': OTCDaily.f_sbl_flow_volume.借券賣出_賣出_股數,
        '借券當日還券': OTCDaily.f_sbl_flow_volume.借券賣出_還券_股數,
        '借券當日調整數額': OTCDaily.f_sbl_flow_volume.借券賣出_調整_股數,
        '借券當日餘額': OTCDaily.f_sbl_balance_volume.借券賣出_餘額_股數,
    },
    True
)

class VERSION_2(SUM, base.VERSION_2):
    pass
    
version_2 = VERSION_2(
    OTCDaily,
    {
        '融券賣出': OTCDaily.f_short_flow_volume.融券_賣出_股數,
        '融券買進': OTCDaily.f_short_flow_volume.融券_買進_股數,
        '融券現券': OTCDaily.f_short_flow_volume.融券_現償_股數,
        '融券當日餘額': OTCDaily.f_short_balance_volume.融券_餘額_股數,
        '借券賣出當日賣出': OTCDaily.f_sbl_flow_volume.借券賣出_賣出_股數,
        '借券賣出當日還券': OTCDaily.f_sbl_flow_volume.借券賣出_還券_股數,
        '借券賣出當日調整數額': OTCDaily.f_sbl_flow_volume.借券賣出_調整_股數,
        '借券賣出當日餘額': OTCDaily.f_sbl_balance_volume.借券賣出_餘額_股數,
    },
    True
)