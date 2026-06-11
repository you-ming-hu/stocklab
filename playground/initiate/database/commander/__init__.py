import sqlite3

from functools import wraps

def execute_many(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        command, parameters, execute = func(self, *args, **kwargs)
        if execute:
            with sqlite3.connect(self.path) as conn:
                conn.executemany(
                    command,
                    parameters
                )
        return command, parameters
    return wrapper

def execute_lines(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        commands, execute = func(self, *args, **kwargs)
        if execute:
            with sqlite3.connect(self.path) as conn:
                for cmd in commands():
                    conn.execute(cmd)
        return commands
    return wrapper

def execute_line(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        command, execute = func(self, *args, **kwargs)
        if execute:
            with sqlite3.connect(self.path) as conn:
                conn.execute(command)
        return command
    return wrapper

class Commander:
    def __init__(self, path):
        self.path = path
    
    @execute_many
    def save_df(self, name, schema, data, execute=True):
        columns = ", ".join(schema.keys())
        placeholders = ", ".join("?" for _ in schema)
        command = f"""
            INSERT OR IGNORE INTO {name} ({columns})
            VALUES ({placeholders})
        """
        parameters = [tuple(row.values()) for row in data]
        return command, parameters, execute
    
    @execute_line
    def get_table_columns(self, table, execute=True):
        return f"PRAGMA table_info({table.__name__})", execute
            
    @execute_lines
    def create_table(self, table, execute=True):
        create = [f'{v} {v.sqltype}' for v in table.cols.values()]
        create.append(f'PRIMARY KEY ({", ".join(table.__primary_keys__)})')
        create = "    " + ",\n    ".join(create)
        create = [f'CREATE TABLE IF NOT EXISTS {table.__name__} (\n{create}\n);']
        add_additional_index = [f'CREATE INDEX IF NOT EXISTS idx_{n} ON {table.__name__} ({n});' for n in table.__additional_index__]
        
        add_new_columns = set(table.cols.values()) - set(row[1] for row in self.get_table_columns(table))
        add_new_columns = [f'ALTER TABLE {table.__name__} ADD COLUMN {col} {col.sqltype}' for col in add_new_columns]

        commands = create + add_additional_index + add_new_columns
        return commands, execute