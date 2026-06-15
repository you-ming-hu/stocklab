from ..base import Source
from ... import schema

import pathlib
from bs4 import BeautifulSoup
import re
import pandas as pd

class STOCKS_STAGE_1(Source):
    def open(self, file):
        content = pathlib.Path(file).read_text('utf-8')
        content = BeautifulSoup(content, "html.parser")
        return content
    
    def check_empty(self, content):
        if len(content.find('body').find_all('table')) == 0:
            assert 'Sorry, the page you requested was not found' in content.text
            return True
        else:
            return False
    
    def to_df(self, content):
        tables = content.find('body').find_all('table')

        dfs = []

        head_cols = ['代號','證券名稱','收盤價','漲跌','開盤價','最高價','最低價','均價','成交股數','成交金額','成交筆數','最後委買價','最後委賣價']
        head_idx = 4
        for table in tables:
            lines = re.sub(r'\n+', '\n', table.text.replace('<RSTA3104>','')).replace(' ','').split('＊＊＊＊＊管理股票＊＊＊＊＊')[0].strip().split('\n')
            lines = [l.strip() for l in lines]
            columns = lines[head_idx:head_idx+len(head_cols)]

            data = lines[head_idx+len(head_cols):-1]
            data = [data[i:i+len(head_cols)] for i in range(0,len(data),len(head_cols))]

            if not '<RSTA3104>' in table.text:
                assert columns == head_cols
            else:
                for drop, d in enumerate(data):
                    if not re.search(r'\d', d[0]):
                        break
                data = data[:drop]
            
            df = pd.DataFrame(columns=head_cols, data=data)
            
            if '<RSTA3104>' in table.text:
                df['證券名稱'] = 'lost'

            dfs.append(df)

        complete_df = pd.concat(dfs, axis=0)
        return complete_df
        
    def format_dtype(self, df):
        cols = self.table.cols

        for name in [cols['代號'], cols['名稱']]:
            df[name] = df[name].str.replace(' ','').replace('*','')

        for name in [cols['交易股數'], cols['交易筆數'], cols['交易金額']]:
            df[name] = df[name].str.replace(',','').astype(int)

        for name in [cols['開盤價'], cols['最高價'], cols['最低價'], cols['收盤價']]:
            df[name] = df[name].str.replace(',','').astype(float)
        return df
    
    def add_other_columns(self, df):
        df[self.table.f_stock_info.市場別] = 'OTC'
        return df

stocks_stage_1 = STOCKS_STAGE_1(
    schema.tables.StockDaily,
    {
        '代號': schema.tables.StockDaily.f_stock_info.代號,
        '證券名稱': schema.tables.StockDaily.f_stock_info.名稱,
        '開盤價': schema.tables.StockDaily.f_techicals.開盤價,
        '最高價': schema.tables.StockDaily.f_techicals.最高價,
        '最低價': schema.tables.StockDaily.f_techicals.最低價,
        '收盤價': schema.tables.StockDaily.f_techicals.收盤價,
        '成交股數': schema.tables.StockDaily.f_techicals.交易股數,
        '成交金額': schema.tables.StockDaily.f_techicals.交易金額,
        '成交筆數': schema.tables.StockDaily.f_techicals.交易筆數
    },
    True
)