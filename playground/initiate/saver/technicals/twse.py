from .. import Saver
from ..specs import market_type, terms

import json
import pandas as pd

class Stocks(Saver):
    
    def open(self, file):
        with open(file, encoding="utf-8") as f:
            content = json.load(f)
        return content
    
    def check_empty(self, content):
        return not 'tables' in content
    
    def to_df(self, content):
        target_table = None
        for table in content['tables']:
            if '每日收盤行情' in table.get('title',''):
                target_table = table
        assert target_table is not None
        df = pd.DataFrame(
            target_table['data'],
            columns=target_table['fields'],
        )
        return df
        
    def format_dtype(self, df):
        for name in [terms.代號,terms.名稱]:
            df[name] = df[name].str.replace(' ','').replace('*','')

        for name in [terms.交易股數,terms.交易筆數,terms.交易金額]:
            df[name] = df[name].str.replace(',','').astype(int)

        for name in [terms.開盤價,terms.最高價,terms.最低價,terms.收盤價]:
            df[name] = df[name].str.replace(',','').replace('--',pd.NA).astype(float)
        return df
    
    def add_other_columns(self, df):
        df[self.schema.market] = market_type.twse
        return df

stocks = Stocks(
    'stocks',
    ('id','date'),
    {
        '證券代號': terms.代號, 
        '證券名稱': terms.名稱,
        '成交股數': terms.交易股數,
        '成交筆數': terms.交易筆數,
        '成交金額': terms.交易金額,
        '開盤價': terms.開盤價,
        '最高價': terms.最高價,
        '最低價': terms.最低價,
        '收盤價': terms.收盤價
    },
    ['date']
)