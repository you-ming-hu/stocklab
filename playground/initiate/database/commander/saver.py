import pandas as pd

from .decorators import *
from .utils import list2text
from ..sources.base import Source
from ..schema.tables.base import DataTimestampTable, UpdateTimestampTable

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

    def register_save_date(self, df:pd.DataFrame):
        if issubclass(self.item.table, DataTimestampTable):
            key = self.item.table.f_datatimestamp.添加日期
        elif issubclass(self.item.table, UpdateTimestampTable):
            key = self.item.table.f_updatetimestamp.更新日期
        df[key] = self.session_date
        return df
    
    @exec_many
    def save_df(self, df:pd.DataFrame, execute=True):
        df = self.register_save_date(df)
        data = [self.item.pydatamodel(**row.to_dict()).model_dump(exclude_unset=True) for _,row in df.iterrows()]
        
        columns = list(data[0].keys())
        table = self.item.table
        name = table.__name__
        command = [
            f'INSERT INTO {name} {list2text(columns)}',
            f'VALUES {list2text(["?"]*len(columns))}',
            f'ON CONFLICT {list2text(table.__primary_keys__)}',
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
        
        create = [f'\t{v} {v.sqltype}' for v in table.cols.values()]
        create = create + [f'PRIMARY KEY {list2text(table.__primary_keys__)}']
        create = ',\n'.join(create)
        create = [
            f'CREATE TABLE {name} (',
            create,
            ')'
        ]
        create = ['\n'.join(create) + ';']

        add_additional_index = [f'CREATE INDEX idx_{n} ON {name} ({n});' for n in table.__additional_index__]         
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
        add_new_columns = set(table.cols.values()) - set(row[1] for row in self.get_table_columns(table))
        add_new_columns = [f'ALTER TABLE {name} ADD COLUMN {col} {col.sqltype};' for col in table.cols.values() if col in add_new_columns]
        add_additional_index = [f'CREATE INDEX IF NOT EXISTS idx_{n} ON {table.__name__} ({n});' for n in table.__additional_index__]     
        commands = add_new_columns + add_additional_index
        return commands, execute

    def update_database(self):
        print(f'{self.item.table.__name__} start')
        for file in self.item.path.iterdir():
            print(f'processing: {file.stem}')
            df = self.item.get_df(file.name)
            if df is None:
                continue
            self.save_df(df)
        print(f'{self.item.table.__name__} finished')