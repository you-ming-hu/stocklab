from ..base import Source
from ...schema.tables import DayOff

from bs4 import BeautifulSoup
import pandas as pd
import re

class VERSION_0(Source):

    def open(self, file):
        return super().open(file, 'json', True)

    def check_empty(self, content):
        content, file = content
        return content['stat'] != 'ok'
        
    def to_df(self, content):
        content, file = content
        year = file.stem[:4]
        table = BeautifulSoup(content['data']['html'], 'lxml')
        table = table.find_all('table')[1].find_all('tr')
        columns = [i.text for i in table[0].find_all('th')]
        assert columns == ['紀念節日名稱', '日期', '星期', '說明']
        data = [[i.text for i in r.find_all('td')] for r in table[1:]]
        rows = []
        for d in data:
            if len(d) == 1:
                continue
            pattern = r'(\d{1,2})月(\d{1,2})日(?:（(\d{2,3})年）)?'
            dates = re.findall(pattern, d[1])
            for date in dates:
                month, day, y = date
                if y != '':
                    if int(y) + 1911 > int(year):
                        continue
                rows.append(['/'.join([year,month,day]), d[0]])
        df = pd.DataFrame(columns=['日期','紀念節日名稱'], data=rows)
        return df

    def format_dtype(self, df):
        date_cols = [
            DayOff.f_datatimestamp.資料日期
        ]
        str_cols = [
            DayOff.f_reason.原因,
        ]
        df = super().format_dtype(df, str_cols=str_cols, date_cols=date_cols)
        return df

version_0 = VERSION_0(
    DayOff,
    {
        '日期': DayOff.f_datatimestamp.資料日期, 
        '紀念節日名稱': DayOff.f_reason.原因,
    },
    False
)

class VERSION_1(Source):

    def check_empty(self, content):
        return content['stat'] != 'ok'
    
    def format_dtype(self, df):
        date_cols = [
            DayOff.f_datatimestamp.資料日期
        ]
        str_cols = [
            DayOff.f_reason.原因,
        ]
        df = super().format_dtype(df, str_cols=str_cols, date_cols=date_cols)
        return df

version_1 = VERSION_1(
    DayOff,
    {
        '日期': DayOff.f_datatimestamp.資料日期, 
        '名稱': DayOff.f_reason.原因
    },
    False
)