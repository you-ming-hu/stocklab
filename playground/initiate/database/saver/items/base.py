import pathlib
import pandas as pd
import pydantic

from ..schema.tables import Table
from ...commander import Commander

sqltype_key = 'sqltype'

class Saver:
    def __init__(self, table: Table, mapping: dict):
        self.table = table
        self.mapping = mapping
        
        self.save_date = pd.Timestamp.today()
        self.save_data_model = pydantic.create_model(
            'save_data_model', **{
                v: (v.pytype, pydantic.Field(json_schema_extra={sqltype_key: v.sqltype}))
                for v in mapping.values()
            }
        )
    
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
    
    def add_source(self, source):
        self.source = pathlib.Path(source)

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
        df[self.table.f_general.資料日期] = pd.Timestamp(date)
        return df
    
    def standardize(self, content, date):
        df = self.to_df(content)
        df = self.keep_interest(df)
        df = self.rename_columns(df)
        df = self.format_dtype(df)
        df = self.drop_incomplete(df)
        df = self.add_data_date(df, date)
        df = self.add_other_columns(df)
        return df

    def get_df(self, file):
        file = self.source.joinpath(file)
        content = self.open(file)
        if self.check_empty(content):
            return False, None
        else:
            df = self.standardize(content, file.stem)
            return True, df
        
    def add_save_date(self, df):
        df[self.table.f_general.新增日期] = pd.Timestamp(self.save_date)
        return df
        
    def save_df(self, df, commander:Commander):
        df = self.add_save_date(df)
        data = [self.save_data_model(**row.to_dict()).model_dump() for _,row in df.iterrows()]
        sqlschema = {
            name: field.json_schema_extra[sqltype_key]
            for name, field in self.save_data_model.model_fields.items()
        }
        commander.save_df(self.table.__name__, sqlschema, data)

    def save_bacth(self, commander):
        for file in self.source.iterdir():
            print(f'processing: {file.stem}')
            success, df = self.get_df(file.name)
            if not success:
                continue
            self.save_df(df, commander)