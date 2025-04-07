import pandas as pd

def get_table_row_count(conn, table_name, source='sqlite'):
    """
    Returns the row count for the given table.
    Works for both SQLite and Snowflake by aliasing the count column.
    """
    query = f"SELECT COUNT(*) AS count FROM {table_name}"
    df = pd.read_sql_query(query, conn)
    # return first value regardless of column name
    return df.iloc[0, 0]    

def get_sample_data(conn, table_name, n=20, source='sqlite'):
    query = f"SELECT * FROM {table_name} LIMIT {n}"
    df = pd.read_sql_query(query, conn)
    return df

def get_table_schema(conn, table_name, source='sqlite'):
    if source == 'sqlite':
        query = f"PRAGMA table_info({table_name});"
        schema = pd.read_sql_query(query, conn)
        return schema[['name', 'type', 'notnull', 'dflt_value']]
    elif source == 'snowflake':
        query = f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = '{table_name.upper()}'
        ORDER BY ordinal_position;
        """
        schema = pd.read_sql_query(query, conn)
        return schema
    else:
        raise ValueError("Unsupported source type.")

def get_snowflake_schemas(conn):
    """
    Returns a list of available schemas in the connected Snowflake database.
    """
    query = "SELECT schema_name FROM information_schema.schemata"
    df = pd.read_sql_query(query, conn)
    return sorted(df['SCHEMA_NAME'].tolist())


def get_table_list(conn, source='sqlite', schema=''):
    if source == 'sqlite':
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        df = pd.read_sql_query(query, conn)
        return df['name'].tolist()
    elif source == 'snowflake':
        query = f"""
        SELECT LOWER(table_name) AS name
        FROM information_schema.tables
        WHERE table_schema = '{schema.upper()}'
        """
        df = pd.read_sql_query(query, conn)
        return df['NAME'].tolist()