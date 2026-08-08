from ...base import Source

import pandas as pd
import pathlib
from bs4 import BeautifulSoup
import re
import json

class MARGIN_V0(Source):
    def open(self, file):
        content = pathlib.Path(file).read_text('utf-8')
        return content, file
    
    def to_df(self, content):
        content, file = content
        if (file.stem <= '20041026') and (file.stem != '20041019'):
            content = BeautifulSoup(content, "html.parser")
            tables = content.find_all('table')
            cols = [
                '代號', '股票名稱', 
                '前日融資餘額', '融資買進', '融資賣出', '現金償還', '本日融資餘額', '限額', 
                '前日融券餘額', '融券賣出', '融券買進', '現券償還', '本日融券餘額'
            ]
            col_count = len(cols)
            total_lines = []

            for table in tables:
                lines = re.sub(r'\n+', '\n', table.text).replace(' ','').split('<OMBALK>')[1].strip()
                total_lines.append(lines)
            total_lines = '\n'.join(total_lines)
            total_lines = total_lines.split('\n')
            sep = total_lines.index('合計(張)')
            stocks = total_lines[:sep]
            assert stocks[:col_count] == cols
            stocks = [stocks[i:i+col_count] for i in range(0,len(stocks),col_count)]
            stocks = pd.DataFrame(columns=cols, data=stocks[1:])
            stocks = stocks.loc[stocks['代號'] != '代號']

            market = total_lines[sep:]
            sep = market.index('融資金(仟元)')
            market_volume = market[:sep]
            market_value = market[sep:]
            cols = [
                '類型', 
                '前日融資餘額', '融資買進', '融資賣出', '現金償還', '本日融資餘額', '前日融券餘額', 
                '融券賣出', '融券買進', '現券償還', '本日融券餘額'
            ]
            market = pd.DataFrame(columns=cols, data=[market_volume, market_value])

            complete_df = stocks, market
        else:
            df = pd.read_html(file)[0]

            template = set([
                '股票 代號股票 代號','股票 名稱股票 名稱',
                '前日融資餘額','融資買進','融資賣出','現金償還','本日融資餘額','本日融資屬證金部分',
                '融資限額','前日融券餘額','融券買進','融券賣出','現券償還','本日融券餘額','本日融券屬證金部分',
                '資券 相抵(張)資券 相抵(張)','備註備註','nan'
            ])
            cols = (df.loc[3]+df.loc[4]).fillna('nan').tolist()
            assert set(cols) <= template

            mapping = {
                '股票 代號股票 代號':'代號',
                '股票 名稱股票 名稱':'股票名稱',
                '融資限額':'限額'
            }

            cols = [mapping.get(c, c) for c in cols]

            df.columns = cols
            sep = df.iloc[:,0].to_list().index('合計 (張)')
            
            stocks = df.iloc[:sep].loc[5:]
            
            market = df.iloc[sep:,1:]
            if len(market) == 4:
                market_volume = market.iloc[:2].reset_index(drop=True)
                market_value = market.iloc[2:].reset_index(drop=True)
                market = [market_volume, market_value]
                for m in market:
                    for i in range(2):
                        m.loc[i,m.loc[i].duplicated(keep='last')] = pd.NA
                market = pd.concat([m.bfill().loc[[0]] for m in market])
            market = market.dropna(axis=1,how='all')
            market = market.reset_index(drop=True)
            market.columns = ['類型'] + market.columns.to_list()[1:]

            complete_df = stocks, market
            
        return complete_df
    
    def check_empty(self, content):
        text, file = content
        return 'Sorry, the page you requested was not found' in text
    
class MARGIN_V1(Source):
    def open(self, file):
        with open(file, encoding='utf-8') as f:
            content = json.load(f)
        return content, file
    
    def to_df(self, content):
        content, file = content
        assert content['date'] == file.stem
        assert len(content['tables']) == 1
        content = content['tables'][0]
        assert content['title'] == '上櫃股票融資融券餘額'
        return content

    def check_empty(self, content):
        content, file = content
        if 'tables' in content:
            return len(content['tables'][0]['data']) == 0
        else:
            return content['stat'] =='很抱歉，沒有符合條件的資料!'
    
class SHORT_SBL_VOLUME(Source):
    
    def check_empty(self, content):
        return len(content['tables'][0]['data']) == 0
    
    def to_df(self, content):
        content = content['tables'][0]
        df = pd.DataFrame(
            data=content['data'],
            columns=content['fields'],
        )
        return df
    
class SHORT_SBL_VALUE(Source):
    
    def check_empty(self, content):
        return len(content['tables'][0]['data']) == 0
    
    def to_df(self, content):
        content = content['tables'][0]
        df = pd.DataFrame(
            data=content['data'],
            columns=content['fields'],
        )
        return df

class SUM(Source):
    def format_dtype(self, df):
        df = super().format_dtype(df, int_cols=df.columns)
        df = df.sum(axis=0).to_frame().T
        return df