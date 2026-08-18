from .utils import Field as F
from .utils import FieldGroup as FG

class Index(FG):
    代號 = F('id', str)

class Info(FG):
    代號 = F('id', str)
    名稱 = F('name', str)
    市場別 = F('market', str)
    交易中 = F('active', bool, False)
    總發行股數 = F('total_outstanding_shares', int)

class ShareholdingDistribution(FG):
    零股_人數 = F('below_1_shareholders', int)
    一至五張_人數 = F('between_1_5_shareholders', int)
    五至十張_人數 = F('between_5_10_shareholders', int)
    十至十五張_人數 = F('between_10_15_shareholders', int)
    十五至二十張_人數 = F('between_15_20_shareholders', int)
    二十至三十張_人數 = F('between_20_30_shareholders', int)
    三十至四十張_人數 = F('between_30_40_shareholders', int)
    四十至五十張_人數 = F('between_40_50_shareholders', int)
    五十至一百張_人數 = F('between_50_100_shareholders', int)
    一百至兩百張_人數 = F('between_100_200_shareholders', int)
    兩百至四百張_人數 = F('between_200_400_shareholders', int)
    四百至六百張_人數 = F('between_400_600_shareholders', int)
    六百到八百張_人數 = F('between_600_800_shareholders', int)
    八百到一千張_人數 = F('between_800_1000_shareholders', int)
    千張以上_人數 = F('above_1000_shareholders', int)

    零股_股數 = F('below_1_sum_shares', int)
    一至五張_股數 = F('between_1_5_sum_shares', int)
    五至十張_股數 = F('between_5_10_sum_shares', int)
    十至十五張_股數 = F('between_10_15_sum_shares', int)
    十五至二十張_股數 = F('between_15_20_sum_shares', int)
    二十至三十張_股數 = F('between_20_30_sum_shares', int)
    三十至四十張_股數 = F('between_30_40_sum_shares', int)
    四十至五十張_股數 = F('between_40_50_sum_shares', int)
    五十至一百張_股數 = F('between_50_100_sum_shares', int)
    一百至兩百張_股數 = F('between_100_200_sum_shares', int)
    兩百至四百張_股數 = F('between_200_400_sum_shares', int)
    四百至六百張_股數 = F('between_400_600_sum_shares', int)
    六百到八百張_股數 = F('between_600_800_sum_shares', int)
    八百到一千張_股數 = F('between_800_1000_sum_shares', int)
    千張以上_股數 = F('above_1000_sum_shares', int)