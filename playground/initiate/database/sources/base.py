import pathlib
import pandas as pd
import pydantic

from ..schema.tables.base import Table

class Source:
    def __init__(self, table: Table, mapping: dict, filename_is_data_date=True):
        self.table = table
        self.mapping = mapping
        self.pydatamodel = pydantic.create_model(
            table.__name__,
            **{v: (v.pytype, None)for v in table.cols.values()}
        )
        self.sqldatamodel = {v:v.sqltype for v in table.cols.values()}
        self.filename_is_data_date = filename_is_data_date
    
    def open(self, file):
        raise NotImplementedError
    
    def check_empty(self, content):
        raise NotImplementedError
    
    def to_df(self, content):
        raise NotImplementedError
    
    def format_dtype(self, df):
        raise NotImplementedError
    
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
        df[self.table.f_datatimestamp.資料日期] = pd.Timestamp(date)
        return df
    
    def standardize(self, content, date):
        df = self.to_df(content)
        df = self.keep_interest(df)
        df = self.rename_columns(df)
        df = self.format_dtype(df)
        df = self.drop_incomplete(df)
        if self.filename_is_data_date:
            df = self.add_data_date(df, date)
        df = self.add_other_columns(df)
        return df

    def get_df(self, file):
        file = self.path.joinpath(file)
        content = self.open(file)
        if self.check_empty(content):
            return None
        else:
            df = self.standardize(content, file.stem)
            return df
    