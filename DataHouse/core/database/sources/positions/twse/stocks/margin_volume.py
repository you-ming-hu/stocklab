from .....schema.tables import StockDaily

from ..base import MARGIN_V0

import pandas as pd

class VERSION_0(MARGIN_V0):
    
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
        str_cols= [
            StockDaily.f_stock_info.代號,
        ]
        int_cols = [
            StockDaily.f_margin_flow_volume.融資_買進_股數,
            StockDaily.f_margin_flow_volume.融資_賣出_股數,
            StockDaily.f_margin_flow_volume.融資_現償_股數,
            StockDaily.f_margin_balance_volume.融資_餘額_股數,
            StockDaily.f_margin_limit.融資_次日限額_股數
        ]
        df = super().format_dtype(df, str_cols, int_cols)
        df[int_cols] = df[int_cols] * 1000
        return df

version_0 = VERSION_0(
    StockDaily,
    {
        '股票代號': StockDaily.f_stock_info.代號,
        '融資買進': StockDaily.f_margin_flow_volume.融資_買進_股數,
        '融資賣出': StockDaily.f_margin_flow_volume.融資_賣出_股數,
        '融資現金償還': StockDaily.f_margin_flow_volume.融資_現償_股數,
        '融資今日餘額': StockDaily.f_margin_balance_volume.融資_餘額_股數,
        '融資次一營業日限額': StockDaily.f_margin_limit.融資_次日限額_股數
    },
    True
)