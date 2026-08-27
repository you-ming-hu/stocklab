from ..base import Source
from ...schema.tables import ShareholdingDistribution

import pandas as pd

class VERSION_0(Source):
    def __init__(self, table, filename_is_data_date):        
        self.levels = [
            '零股',
            '一至五張',
            '五至十張',
            '十至十五張',
            '十五至二十張',
            '二十至三十張',
            '三十至四十張',
            '四十至五十張',
            '五十至一百張',
            '一百至兩百張',
            '兩百至四百張',
            '四百至六百張',
            '六百到八百張',
            '八百到一千張',
            '千張以上',
            '調整',
            '總計'
        ]
        self.int_cates = [
            '人數',
            '股數'
        ]
        self.float_cates = [
            '持股比例'
        ]
        mapping = {
            '代號': ShareholdingDistribution.f_index.代號,
            '日期': ShareholdingDistribution.f_datatimestamp.資料日期
        }
        for cate in self.int_cates+self.float_cates:
            for i,level in enumerate(self.levels):
                mapping[f"分級{str(i+1)}-{cate}"] = getattr(ShareholdingDistribution.f_shareholding_distribution, f'{level}_{cate}')
        super().__init__(table, mapping, filename_is_data_date)

    def check_empty(self, content):
        return False

    def to_df(self, content):
        df = pd.DataFrame(content)
        df.columns = [c.replace('\ufeff','') for c in df.columns]
        mapping = {
            '證券代號':'代號', 
            '占集保庫存數比例%':'持股比例', 
            '人數': '人數', 
            '資料日期': '日期', 
            '股數':'股數', 
            '持股分級':'分級'
        }
        df.columns = [mapping[c] for c in df.columns]

        value_cols = [
            "持股比例",
            "人數",
            "股數",
        ]

        df_wide = df.pivot(
            index=["代號", "日期"],
            columns="分級",
            values=value_cols,
        )

        df_wide = df_wide.swaplevel(axis=1).sort_index(axis=1)

        df_wide.columns = [
            f"分級{level}-{column}"
            for level, column in df_wide.columns
        ]

        df_wide = df_wide.reset_index()
        return df_wide

    def format_dtype(self, df):
        date_cols = [
            self.mapping['日期']
        ]
        str_cols = [
            self.mapping['代號']
        ]
        int_cols = [getattr(ShareholdingDistribution.f_shareholding_distribution, f'{l}_{c}') for c in self.int_cates for l in self.levels]
        float_cols = [getattr(ShareholdingDistribution.f_shareholding_distribution, f'{l}_{c}') for c in self.float_cates for l in self.levels]
        df = super().format_dtype(df, str_cols, int_cols, float_cols, date_cols)
        return df

version_0 = VERSION_0(
    ShareholdingDistribution,
    False
)