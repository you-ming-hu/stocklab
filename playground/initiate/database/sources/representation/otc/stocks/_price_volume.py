from ....base import Source
from ..... import schema

import pathlib
from bs4 import BeautifulSoup
import re
import pandas as pd
import json

class PRICE_VOLUME_STAGE_1(Source):
    def open(self, file):
        content = pathlib.Path(file).read_text('utf-8')
        return content, file
    
    def check_empty(self, content):
        content, file = content
        return 'Sorry, the page you requested was not found' in content
    
    def to_df(self, content):
        content, file = content

        if file.stem < '20041028':
            content = BeautifulSoup(content, "html.parser")
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

                if file.stem < '20040202':
                    assert columns == head_cols
                else:
                    for drop, d in enumerate(data):
                        if not re.search(r'\d', d[0]):
                            break
                    data = data[:drop]
            
                df = pd.DataFrame(columns=head_cols, data=data)
                
                if file.stem >= '20040202':
                    df['證券名稱'] = 'lost'

                dfs.append(df)
            complete_df = pd.concat(dfs, axis=0)
        else:
            tables = pd.read_html(file, encoding='utf-8')
            assert len(tables) == 1
            table = tables[0]
            table.columns = table.loc[0]
            table = table.loc[1:]

            if file.stem < '20041125':
                head_cols = [
                    '股票 代號', '證券 名稱', '收盤價', '漲跌', '漲跌', '開盤價', '最高價', '最低價', '均價', 
                    '成交股數', '成交金額(元)', '成交筆數', '最後 委買價', '最後 委賣價', 
                    '發行股數', '次日 參考價', '次日 漲停價', '次日 跌停價'
                ]
                assert table.columns.to_list() == head_cols
            else:
                head_cols = [
                    '股票 代號', '證券 名稱', '收盤價', '收盤價', '漲跌', '漲跌', '開盤價', '最高價', '最低價', '均價', 
                    '成交股數', '成交金額(元)', '成交筆數', '最後 委買價', '最後 委賣價', 
                    '發行股數', '次日 參考價', '次日 漲停價', '次日 跌停價'
                ]
                assert table.columns.to_list() == head_cols
                head_cols[2] = '收盤價-drop'
                table.columns = head_cols

            mapping = {
                '股票 代號':'代號',
                '證券 名稱':'證券名稱',
                '收盤價':'收盤價',
                '開盤價':'開盤價',
                '最高價':'最高價',
                '最低價':'最低價',
                '成交股數':'成交股數',
                '成交金額(元)':'成交金額',
                '成交筆數':'成交筆數',
            }

            table = table[list(mapping.keys())]
            table.columns = [mapping[c] for c in table.columns]

            table = table.loc[(table['代號']!='管 理 股 票').cumprod() == 1]
            if not table.empty:
                complete_df = table
            else:
                complete_df = None

        return complete_df
        
    def format_dtype(self, df):
        cols = self.table.cols

        stock_info_cols= [cols['代號'], cols['名稱']]
        volume_cols = [cols['交易股數'], cols['交易筆數'], cols['交易金額']]
        price_cols = [cols['開盤價'], cols['最高價'], cols['最低價'], cols['收盤價']]

        for name in stock_info_cols:
            df[name] = df[name].str.replace(' ','').replace('*','')

        for name in volume_cols:
            df[name] = df[name].str.replace(',','').replace('','0').astype(int)

        for name in price_cols:
            df[name] = df[name].map(lambda t: re.sub(r'[^0-9.]','',t)).replace('',pd.NA).astype(float)
        
        df = df.loc[~(df[volume_cols+price_cols] == 0).all(axis=1)]
        return df
    
    def add_other_columns(self, df):
        df[self.table.f_stock_info.市場別] = 'OTC'
        return df

price_volume_stage_1 = PRICE_VOLUME_STAGE_1(
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

class PRICE_VOLUME_STAGE_2(PRICE_VOLUME_STAGE_1):
    
    def open(self, file):
        content = pathlib.Path(file)
        return content
    
    def check_empty(self, content):
        return content.read_text('utf-8') == '\ufeff'
    
    def to_df(self, content):
        soup = BeautifulSoup(content.read_text('utf-8'), 'html.parser')

        tables = []
        for table in soup.find_all("table"):
            rows = []

            for tr in table.find_all("tr"):
                row = [
                    td.get_text(strip=True)
                    for td in tr.find_all(["td", "th"])
                ]
                rows.append(row)

            tables.append(rows)

        assert len(tables) == 3

        head_col = [
            '代號','名稱',
            '收盤','漲跌','開盤','最高','最低','均價',
            '成交股數','成交金額(元)','成交筆數',
            '最後買價','最後賣價','發行股數','次日參考價','次日漲停價','次日跌停價'
        ]
        columns = tables[0][1]
        assert columns == head_col

        df = pd.DataFrame(columns=columns, data=tables[1])
        df = df.loc[(df['代號']!='管理股票').cumprod() == 1]

        return df

price_volume_stage_2 = PRICE_VOLUME_STAGE_2(
    schema.tables.StockDaily,
    {
        '代號': schema.tables.StockDaily.f_stock_info.代號,
        '名稱': schema.tables.StockDaily.f_stock_info.名稱,
        '收盤': schema.tables.StockDaily.f_techicals.收盤價,
        '開盤': schema.tables.StockDaily.f_techicals.開盤價,
        '最高': schema.tables.StockDaily.f_techicals.最高價,
        '最低': schema.tables.StockDaily.f_techicals.最低價,
        '成交股數': schema.tables.StockDaily.f_techicals.交易股數,
        '成交金額(元)': schema.tables.StockDaily.f_techicals.交易金額,
        '成交筆數': schema.tables.StockDaily.f_techicals.交易筆數
    },
    True
)

class PRICE_VOLUME_STAGE_3(PRICE_VOLUME_STAGE_1):
    
    def open(self, file):
        with open(file, encoding='utf-8') as f:
            content = json.load(f)
        assert content['date'] == file.stem
        assert len(content['tables']) == 2
        content = content['tables'][0]
        assert content['title'] == '上櫃股票行情'
        return content
    
    def check_empty(self, content):
        return len(content['data']) == 0
    
    def to_df(self, content):
        df = pd.DataFrame(
            data=content['data'],
            columns=content['fields'],
        )
        return df
    
price_volume_stage_3 = PRICE_VOLUME_STAGE_3(
    schema.tables.StockDaily,
    {
        '代號': schema.tables.StockDaily.f_stock_info.代號,
        '名稱': schema.tables.StockDaily.f_stock_info.名稱,
        '收盤': schema.tables.StockDaily.f_techicals.收盤價,
        '開盤': schema.tables.StockDaily.f_techicals.開盤價,
        '最高': schema.tables.StockDaily.f_techicals.最高價,
        '最低': schema.tables.StockDaily.f_techicals.最低價,
        '成交股數': schema.tables.StockDaily.f_techicals.交易股數,
        '成交金額(元)': schema.tables.StockDaily.f_techicals.交易金額,
        '成交筆數': schema.tables.StockDaily.f_techicals.交易筆數
    },
    True
)