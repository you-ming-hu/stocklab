from ..schema.tables.base import Table

import pathlib
import pandas as pd
import pydantic
import json
import re

class Source:
    def __init__(self, table: Table, mapping: dict, filename_is_data_date=True):
        self.table = table
        self.mapping = mapping
        self.pydatamodel = pydantic.create_model(
            table.__name__,
            **{v: (v.pytype, None)for v in table.columns.values()}
        )
        self.sqldatamodel = {v:v.sqltype for v in table.columns.values()}
        self.filename_is_data_date = filename_is_data_date
    
    def open(self, file, method='json', return_path=False):
        if method == 'json':
            with open(file, encoding="utf-8") as f:
                content = json.load(f)
        elif method == 'text':
            content = pathlib.Path(file).read_text('utf-8')
        else:
            assert False, f'not recognizable method: {method}'
        if return_path:
            return content, file
        else:
            return content
    
    def check_empty(self, content):
        raise NotImplementedError
    
    def to_df(self, content):
        df = pd.DataFrame(
            data=content['data'],
            columns=content['fields'],
        )
        return df
    
    def format_dtype(self, df, str_cols=[], int_cols=[], float_cols=[], date_cols=[], taiwan_date_cols=[]):
        for c in str_cols:
            df[c] = df[c].str.replace(' ','').str.replace('*','')
        for cols,dtype in [[int_cols,int],[float_cols,float]]:
            for c in cols:
                if df[c].dtype == dtype:
                    continue
                df[c] = df[c].map(lambda t: re.sub(r'[^0-9.-]','',t))
                df[c] = df[c].map(lambda t: t if bool(re.match(r'^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)$',t)) else pd.NA)
                df[c] = df[c].map(lambda x: dtype(x) if not pd.isna(x) else x)
        for c in date_cols:
            df[c] = df[c].apply(pd.Timestamp)
        for c in taiwan_date_cols:
            ymd = df[c].str.split('/', expand=True).astype(int)
            ymd[0] = ymd[0] + 1911
            ymd = ymd.apply(lambda cols: pd.Timestamp(f'{cols[0]}{cols[1]:0>2}{cols[2]:0>2}'),axis=1)
            df[c] = ymd
        return df
    
    def add_other_columns(self, df):
        return df
    
    def add_path(self, path):
        self.path = pathlib.Path(path)

    def add_database_path(self, path, name):
        self.database_path = pathlib.Path(path, name).with_suffix('.db')
    
    def keep_interest(self, df):
        return df.loc[:, [c for c in self.mapping.keys()]]
    
    def rename_columns(self, df):
        df.columns = [self.mapping[c] for c in df.columns]
        return df
    
    def drop_incomplete(self, df):
        return df.dropna()
    
    def add_data_date(self, df, date):
        assert not self.table.f_datatimestamp.資料日期 in df
        df[self.table.f_datatimestamp.資料日期] = pd.Timestamp(date)
        return df
    
    def standardize(self, content, file, drop_incomplete=True):
        df = self.to_df(content)
        if not df is None: 
            df = self.keep_interest(df)
            df = self.rename_columns(df)
            df = self.format_dtype(df)
            if drop_incomplete:
                df = self.drop_incomplete(df)
            if self.filename_is_data_date:
                df = self.add_data_date(df, file.stem)
            else:
                assert self.table.f_datatimestamp.資料日期 in df
            df = self.add_other_columns(df)
        return df

    def get_df(self, file):
        file = self.path.joinpath(file)
        content = self.open(file)
        if self.check_empty(content):
            return None
        else:
            df = self.standardize(content, file)
            return df

class SUM:
    
    def format_dtype(self, df):
        df = Source.format_dtype(self, df, int_cols=df.columns)
        df = df.sum(axis=0).to_frame().T
        return df