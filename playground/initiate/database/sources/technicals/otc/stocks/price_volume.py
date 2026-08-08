from ..base import OTC_STOCKS
from ..... import schema

from bs4 import BeautifulSoup
import re
import pandas as pd

class VERSION_0(OTC_STOCKS):

    def open(self, file):
        return super().open(file, 'text')
    
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
        
version_0 = VERSION_0(
    schema.tables.StockDaily,
    {
        '代號': schema.tables.StockDaily.f_stock_info.代號,
        '證券名稱': schema.tables.StockDaily.f_stock_info.名稱,
        '開盤價': schema.tables.StockDaily.f_technicals_price.開盤價,
        '最高價': schema.tables.StockDaily.f_technicals_price.最高價,
        '最低價': schema.tables.StockDaily.f_technicals_price.最低價,
        '收盤價': schema.tables.StockDaily.f_technicals_price.收盤價,
        '成交股數': schema.tables.StockDaily.f_technicals_volume.交易股數,
        '成交金額': schema.tables.StockDaily.f_technicals_volume.交易金額,
        '成交筆數': schema.tables.StockDaily.f_technicals_volume.交易筆數
    },
    True
)

class VERSION_1(OTC_STOCKS):

    def open(self, file):
        return super().open(file, 'text')
    
    def check_empty(self, content):
        content, file = content
        return content == '\ufeff'
    
    def to_df(self, content):
        content, file = content

        soup = BeautifulSoup(file.read_text('utf-8'), 'html.parser')

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

version_1 = VERSION_1(
    schema.tables.StockDaily,
    {
        '代號': schema.tables.StockDaily.f_stock_info.代號,
        '名稱': schema.tables.StockDaily.f_stock_info.名稱,
        '收盤': schema.tables.StockDaily.f_technicals_price.收盤價,
        '開盤': schema.tables.StockDaily.f_technicals_price.開盤價,
        '最高': schema.tables.StockDaily.f_technicals_price.最高價,
        '最低': schema.tables.StockDaily.f_technicals_price.最低價,
        '成交股數': schema.tables.StockDaily.f_technicals_volume.交易股數,
        '成交金額(元)': schema.tables.StockDaily.f_technicals_volume.交易金額,
        '成交筆數': schema.tables.StockDaily.f_technicals_volume.交易筆數
    },
    True
)

class VERSION_2(OTC_STOCKS):
    
    def open(self, file):
        return super().open(file, 'json', False)
    
    def check_empty(self, content):
        return len(content['tables'][0]['data']) == 0
    
    def to_df(self, content):
        assert len(content['tables']) == 2
        table = content['tables'][0]
        df = super().to_df(table)
        return df
    
version_2 = VERSION_2(
    schema.tables.StockDaily,
    {
        '代號': schema.tables.StockDaily.f_stock_info.代號,
        '名稱': schema.tables.StockDaily.f_stock_info.名稱,
        '收盤': schema.tables.StockDaily.f_technicals_price.收盤價,
        '開盤': schema.tables.StockDaily.f_technicals_price.開盤價,
        '最高': schema.tables.StockDaily.f_technicals_price.最高價,
        '最低': schema.tables.StockDaily.f_technicals_price.最低價,
        '成交股數': schema.tables.StockDaily.f_technicals_volume.交易股數,
        '成交金額(元)': schema.tables.StockDaily.f_technicals_volume.交易金額,
        '成交筆數': schema.tables.StockDaily.f_technicals_volume.交易筆數
    },
    True
)