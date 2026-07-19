from ..... import schema

from ...base import MARGIN

import pandas as pd

class VERSION_0(MARGIN):
    def to_df(self, content):
        for table in content['tables']:
            if 'title' in table:
                if '融資融券彙總' in table['title']:
                    break

        head_cols = []
        i = 0
        for group in table['groups']:
            span = group['span']
            title = group['title']
            head_cols.extend([title+n for n in table['fields'][i:i+span]])
            i += span

        assert len(head_cols) == 16
        df = pd.DataFrame(columns=head_cols, data=table['data'])
        return df
        
    def format_dtype(self, df):
        stock_info_cols= [
            schema.tables.StockDaily.f_stock_info.代號,
        ]

        volume_cols = [
            schema.tables.StockDaily.f_margin_flow_volume.融資_買進_股數,
            schema.tables.StockDaily.f_margin_flow_volume.融資_賣出_股數,
            schema.tables.StockDaily.f_margin_flow_volume.融資_現償_股數,
            schema.tables.StockDaily.f_margin_balance_volume.融資_餘額_股數,
            schema.tables.StockDaily.f_margin_limit.融資_次日限額_股數
        ]

        for name in stock_info_cols:
            df[name] = df[name].str.replace(' ','').replace('*','')

        for name in volume_cols:
            df[name] = df[name].str.replace(',','').astype(int) * 1000

        return df

version_0 = VERSION_0(
    schema.tables.StockDaily,
    {
        '股票代號': schema.tables.StockDaily.f_stock_info.代號,
        '融資買進': schema.tables.StockDaily.f_margin_flow_volume.融資_買進_股數,
        '融資賣出': schema.tables.StockDaily.f_margin_flow_volume.融資_賣出_股數,
        '融資現金償還': schema.tables.StockDaily.f_margin_flow_volume.融資_現償_股數,
        '融資今日餘額': schema.tables.StockDaily.f_margin_balance_volume.融資_餘額_股數,
        '融資次一營業日限額': schema.tables.StockDaily.f_margin_limit.融資_次日限額_股數
    },
    True
)