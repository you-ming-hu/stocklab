from ..... import schema

from ..stocks import margin_volume as base

import pandas as pd

class VERSION_0(base.VERSION_0):
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

market = VERSION_0(
    schema.tables.TWSEDaily,
    {
        '融資(交易單位)買進': schema.tables.TWSEDaily.f_margin.融資買進股數,
        '融資(交易單位)賣出': schema.tables.TWSEDaily.f_margin.融資賣出股數,
        '融資(交易單位)現金(券)償還': schema.tables.TWSEDaily.f_margin.融資現償股數,
        '融資(交易單位)今日餘額': schema.tables.TWSEDaily.f_margin.融資餘額股數,
        '融資金額(仟元)買進': schema.tables.TWSEDaily.f_margin_additional.融資買進金額,
        '融資金額(仟元)賣出': schema.tables.TWSEDaily.f_margin_additional.融資賣出金額,
        '融資金額(仟元)現金(券)償還': schema.tables.TWSEDaily.f_margin_additional.融資現償金額,
        '融資金額(仟元)今日餘額': schema.tables.TWSEDaily.f_margin_additional.融資餘額金額
    },
    True
)
