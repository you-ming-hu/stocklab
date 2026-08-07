from ..... import schema

from ..base import MARGIN_V0, MARGIN_V1

import pandas as pd

class VERSION_0(MARGIN_V0):
    def to_df(self, content):
        stocks, df = super().to_df(content)
        volume = df.loc[0]
        value = df.loc[1]

        for v in [volume, value]:
            v.index = v.index+v['類型']

        df = pd.concat([volume.iloc[1:],value.iloc[1:]]).dropna().to_frame().T
        df.columns = [c.replace(' ','') for c in df.columns]
        return df
    
    def format_dtype(self, df):
        df = super().format_dtype(df, int_cols=df.columns)
        df = df * 1000
        return df

version_0 = VERSION_0(
    schema.tables.OTCDaily,
    {
        '融資買進合計(張)': schema.tables.OTCDaily.f_margin_flow_volume.融資_買進_股數,
        '融資賣出合計(張)': schema.tables.OTCDaily.f_margin_flow_volume.融資_賣出_股數,
        '現金償還合計(張)': schema.tables.OTCDaily.f_margin_flow_volume.融資_現償_股數,
        '本日融資餘額合計(張)': schema.tables.OTCDaily.f_margin_balance_volume.融資_餘額_股數,
        '融資買進融資金(仟元)': schema.tables.OTCDaily.f_margin_flow_value.融資_買進_金額,
        '融資賣出融資金(仟元)': schema.tables.OTCDaily.f_margin_flow_value.融資_賣出_金額,
        '現金償還融資金(仟元)': schema.tables.OTCDaily.f_margin_flow_value.融資_現償_金額,
        '本日融資餘額融資金(仟元)': schema.tables.OTCDaily.f_margin_balance_value.融資_餘額_金額
    },
    True
)

class VERSION_1(MARGIN_V1):
    def to_df(self, content):
        content = super().to_df(content)
        df = pd.DataFrame(columns=content['fields'],data=content['summary'])
        
        volume = df.loc[0]
        value = df.loc[1]

        for v in [volume, value]:
            v.index = v.index+v['名稱']

        df = pd.concat([volume.iloc[2:],value.iloc[2:]]).replace('',pd.NA).dropna().to_frame().T
        
        return df
    
    def format_dtype(self, df):
        df = super().format_dtype(df, int_cols=df.columns)
        df = df * 1000
        return df

version_1 = VERSION_1(
    schema.tables.OTCDaily,
    {
        '資買合計(張)': schema.tables.OTCDaily.f_margin_flow_volume.融資_買進_股數,
        '資賣合計(張)': schema.tables.OTCDaily.f_margin_flow_volume.融資_賣出_股數,
        '現償合計(張)': schema.tables.OTCDaily.f_margin_flow_volume.融資_現償_股數,
        '資餘額合計(張)': schema.tables.OTCDaily.f_margin_balance_volume.融資_餘額_股數,
        '資買融資金(仟元)': schema.tables.OTCDaily.f_margin_flow_value.融資_買進_金額,
        '資賣融資金(仟元)': schema.tables.OTCDaily.f_margin_flow_value.融資_賣出_金額,
        '現償融資金(仟元)': schema.tables.OTCDaily.f_margin_flow_value.融資_現償_金額,
        '資餘額融資金(仟元)': schema.tables.OTCDaily.f_margin_balance_value.融資_餘額_金額
    },
    True
)