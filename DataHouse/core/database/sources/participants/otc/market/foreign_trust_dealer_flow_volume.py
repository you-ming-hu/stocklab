from ..... import schema

from ..base import SUM

from ..stocks import foreign_trust_dealer_flow_volume as base

class VERSION_0(SUM, base.VERSION_0):
    pass
    
version_0 = VERSION_0(
    schema.tables.OTCDaily,
    {
        '外資及陸資買股數': schema.tables.OTCDaily.f_foreign_flow_volume.外陸資_買進_股數,
        '外資及陸資賣股數': schema.tables.OTCDaily.f_foreign_flow_volume.外陸資_賣出_股數,
        '投信買進股數': schema.tables.OTCDaily.f_trust_flow_volume.投信_買進_股數,
        '投信賣股數': schema.tables.OTCDaily.f_trust_flow_volume.投信_賣出_股數,
        '自營商買股數': schema.tables.OTCDaily.f_dealer_flow_volume.自營商_買進_股數,
        '自營商賣股數': schema.tables.OTCDaily.f_dealer_flow_volume.自營商_賣出_股數
    },
    True
)
    
class VERSION_1(SUM, base.VERSION_1):
    pass

version_1 = VERSION_1(
    schema.tables.OTCDaily,
    {
        '外資及陸資買股數': schema.tables.OTCDaily.f_foreign_flow_volume.外陸資_買進_股數,
        '外資及陸資賣股數': schema.tables.OTCDaily.f_foreign_flow_volume.外陸資_賣出_股數,
        '投信買進股數': schema.tables.OTCDaily.f_trust_flow_volume.投信_買進_股數,
        '投信賣股數': schema.tables.OTCDaily.f_trust_flow_volume.投信_賣出_股數,
        '自營商(自行買賣)買股數': schema.tables.OTCDaily.f_dealer_flow_volume.自營商_自行買賣_買進_股數,
        '自營商(自行買賣)賣股數': schema.tables.OTCDaily.f_dealer_flow_volume.自營商_自行買賣_賣出_股數,
        '自營商(避險)買股數': schema.tables.OTCDaily.f_dealer_flow_volume.自營商_避險_買進_股數,
        '自營商(避險)賣股數': schema.tables.OTCDaily.f_dealer_flow_volume.自營商_避險_賣出_股數,
    },
    True
)

class VERSION_2(SUM, base.VERSION_2):
    pass

version_2 = VERSION_2(
    schema.tables.OTCDaily,
    {   
        '外資及陸資(不含外資自營商)買進股數': schema.tables.OTCDaily.f_foreign_flow_volume.外陸資_不含外資自營商_買進_股數,
        '外資及陸資(不含外資自營商)賣出股數': schema.tables.OTCDaily.f_foreign_flow_volume.外陸資_不含外資自營商_賣出_股數,
        '外資自營商買進股數': schema.tables.OTCDaily.f_foreign_flow_volume.外資自營商_買進_股數,
        '外資自營商賣出股數': schema.tables.OTCDaily.f_foreign_flow_volume.外資自營商_賣出_股數,
        '外資及陸資買進股數': schema.tables.OTCDaily.f_foreign_flow_volume.外陸資_買進_股數,
        '外資及陸資賣出股數': schema.tables.OTCDaily.f_foreign_flow_volume.外陸資_賣出_股數,
        '投信買進股數': schema.tables.OTCDaily.f_trust_flow_volume.投信_買進_股數,
        '投信賣出股數': schema.tables.OTCDaily.f_trust_flow_volume.投信_賣出_股數,
        '自營商(自行買賣)買進股數': schema.tables.OTCDaily.f_dealer_flow_volume.自營商_自行買賣_買進_股數,
        '自營商(自行買賣)賣出股數': schema.tables.OTCDaily.f_dealer_flow_volume.自營商_自行買賣_賣出_股數,
        '自營商(避險)買進股數': schema.tables.OTCDaily.f_dealer_flow_volume.自營商_避險_買進_股數,
        '自營商(避險)賣出股數': schema.tables.OTCDaily.f_dealer_flow_volume.自營商_避險_賣出_股數,
        '自營商買進股數': schema.tables.OTCDaily.f_dealer_flow_volume.自營商_買進_股數,
        '自營商賣出股數': schema.tables.OTCDaily.f_dealer_flow_volume.自營商_賣出_股數,
    },
    True
)
