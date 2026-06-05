import pathlib
import pandas as pd
import sqlite3

from .specs import table

class Saver:
    def __init__(self, table_name, primary_keys, mapping, other_keys=[]):
        self.table_name = table_name
        self.schema = getattr(table, table_name)
        self.mapping = mapping
        self.primary_key = tuple(str(getattr(self.schema,k)) for k in primary_keys)
        self.other_keys = other_keys
    
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
    
    def add_timestamp(self, df, timestamp):
        df[table.base.date] = pd.Timestamp(timestamp)
        return df
    
    def standardize(self, content, timestamp):
        df = self.to_df(content)
        df = self.keep_interest(df)
        df = self.rename_columns(df)
        df = self.format_dtype(df)
        df = self.drop_incomplete(df)
        df = self.add_timestamp(df, timestamp)
        df = self.add_other_columns(df)
        return df
    
    def execute_single(self, file):
        file = self.source.joinpath(file)
        content = self.open(file)
        if self.check_empty(content):
            return False, None
        else:
            df = self.standardize(content, file.stem)
            return True, df
        
    def save_df(self, df):
        with sqlite3.connect(self.database_path) as conn:
            data = [self.schema.__class__(**row.to_dict()).model_dump() for _,row in df.iterrows()]
            columns = ", ".join(data[0].keys())
            placeholders = ", ".join("?" for _ in data[0])
            sql = f"""
                INSERT OR IGNORE INTO {self.table_name} ({columns})
                VALUES ({placeholders})
            """
            conn.executemany(
                sql,
                [tuple(row.values()) for row in data]
            )

    def execute_batch(self):
        for file in self.source.iterdir():
            print(f'processing: {file.stem}')
            success, df = self.execute_single(file.name)
            if not success:
                continue
            self.save_df(df)

    def to_sqlite_type(self, py_type):
        mapping = {
            int: "INTEGER",
            float: "REAL",
            bool: "INTEGER",
        }
        return mapping.get(py_type, "TEXT")

    def create_sql_table_command(self):
        columns = []
        for name, field in self.schema.model_fields.items():
            dtype = self.to_sqlite_type(field.annotation)
            columns.append(f"{name} {dtype}")
        columns.append(f'PRIMARY KEY ({", ".join(self.primary_key)})')
        cols = "    " + ",\n    ".join(columns)
        command = f'CREATE TABLE IF NOT EXISTS {self.table_name} (\n{cols}\n);'
        return command
    
    def add_sql_table_index_commands(self):
        commands = [f'CREATE INDEX idx_{n} ON {self.table_name} ({n});' for n in self.other_keys]
        return commands
    
    def initialize_sql_table(self):
        with sqlite3.connect(self.database_path) as conn:
            conn.execute(self.create_sql_table_command())
            for command in self.add_sql_table_index_commands():
                conn.execute(command)





    
