def to_sql_tuple(cols):
    if isinstance(cols, str):
        cols = (cols,)
    return f"({', '.join(cols)})"

def create_index_statement(table, cols):
    if isinstance(cols, str):
        cols = (cols,)
    return (
        f"CREATE INDEX IF NOT EXISTS idx_{'_'.join(cols)} "
        f"ON {table} {to_sql_tuple(cols)};"
    )