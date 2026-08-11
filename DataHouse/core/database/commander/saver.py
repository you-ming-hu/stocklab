from .decorators import *
from . import utils
from ..sources.base import Source

import pandas as pd
import sqlite3

class Saver:
    def __init__(self, path):
        self.path = path
        self.session_date = pd.Timestamp.today().date()

    def register(self, item: Source):
        self.item = item
        return self

    def __enter__(self):
        self.conn = sqlite3.connect(self.path)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()
        del self.conn
        del self.item
    
    @exec_many
    def save_df(self, df:pd.DataFrame, execute=True):
        data = [self.item.pydatamodel(**row.to_dict()).model_dump(exclude_unset=True) for _,row in df.iterrows()]
        
        columns = list(data[0].keys())
        table = self.item.table
        name = table.__name__
        command = [
            f'INSERT INTO {name} {utils.to_sql_tuple(columns)}',
            f'VALUES {utils.to_sql_tuple(["?"]*len(columns))}',
            f'ON CONFLICT {utils.to_sql_tuple(table.__primary_keys__)}',
            f'DO UPDATE SET',
        ]
        update = ',\n'.join([f'\t{c} = COALESCE(excluded.{c}, {name}.{c})' for c in df.columns])
        command = command + [update]
        command = '\n'.join(command) + ';'

        parameters = [tuple(row.values()) for row in data]
        return command, parameters, execute
    
    @exec_lines
    def init_table(self, execute=True):
        table = self.item.table
        name = table.__name__
        
        create = [f'\t{v} {v.sqltype}' if v.default is None else f'\t{v} {v.sqltype} DEFAULT {v.default}' for v in table.columns.values()]
        create = create + [f'PRIMARY KEY {utils.to_sql_tuple(table.__primary_keys__)}']
        create = ',\n'.join(create)
        create = [
            f'CREATE TABLE {name} (',
            create,
            ')'
        ]
        create = ['\n'.join(create) + ';']
        
        add_additional_index = [utils.create_index_statement(name, n) for n in table.__additional_index__]
        commands = create + add_additional_index
        return commands, execute
    
    @eval_line
    def get_table_columns(self, execute=True):
        table = self.item.table
        return f"PRAGMA table_info({table.__name__})", execute
    
    @exec_lines
    def update_table(self, execute=True):
        table = self.item.table
        name = table.__name__
        add_new_columns = set(table.columns.values()) - set(row[1] for row in self.get_table_columns(table))
        add_new_columns = [f'ALTER TABLE {name} ADD COLUMN {col} {col.sqltype};' for col in table.columns.values() if col in add_new_columns]
        add_additional_index = [utils.create_index_statement(name, n) for n in table.__additional_index__]
        commands = add_new_columns + add_additional_index
        return commands, execute

    def update_database(self, start_date=None, end_date=None):
        print(f'{self.item.table.__name__} start')
        files = sorted(self.item.path.iterdir())
        if not start_date is None:
            files = [f for f in files if pd.Timestamp(f.stem) >= pd.Timestamp(start_date)]
        if not end_date is None:
            files = [f for f in files if pd.Timestamp(f.stem) <= pd.Timestamp(end_date)]
        for file in files:
            print(f'processing: {file.stem}')
            df = self.item.get_df(file.name)
            if df is None:
                continue
            self.save_df(df)
        print(f'{self.item.table.__name__} finished')