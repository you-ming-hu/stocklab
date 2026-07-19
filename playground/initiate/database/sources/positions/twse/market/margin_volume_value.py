from ..... import schema

from ...base import MARGIN

import pandas as pd

class VERSION_0(MARGIN):
    def to_df(self, content):
        for table in content['tables']:
            if 'title' in table:
                if '信用交易統計' in table['title']:
                    break
        df = pd.DataFrame(columns=table['fields'], data=table['data']).set_index('項目')
        rearrange = {}
        for item, values in df.iterrows():
            for cate, value in values.items():
                rearrange[item+cate] = value
        assert len(rearrange) == 15
        df = pd.Series(rearrange).to_frame().T
        return df
    
    def format_dtype(self, df):
        for name in df.columns:
            df[name] = df[name].str.replace(',','').astype(int) * 1000
        return df

version_0 = VERSION_0(
    schema.tables.TWSEDaily,
    {
        '融資(交易單位)買進': schema.tables.TWSEDaily.f_margin_flow_volume.融資_買進_股數,
        '融資(交易單位)賣出': schema.tables.TWSEDaily.f_margin_flow_volume.融資_賣出_股數,
        '融資(交易單位)現金(券)償還': schema.tables.TWSEDaily.f_margin_flow_volume.融資_現償_股數,
        '融資(交易單位)今日餘額': schema.tables.TWSEDaily.f_margin_balance_volume.融資_餘額_股數,
        '融資金額(仟元)買進': schema.tables.TWSEDaily.f_margin_flow_value.融資_買進_金額,
        '融資金額(仟元)賣出': schema.tables.TWSEDaily.f_margin_flow_value.融資_賣出_金額,
        '融資金額(仟元)現金(券)償還': schema.tables.TWSEDaily.f_margin_flow_value.融資_現償_金額,
        '融資金額(仟元)今日餘額': schema.tables.TWSEDaily.f_margin_balance_value.融資_餘額_金額
    },
    True
)
