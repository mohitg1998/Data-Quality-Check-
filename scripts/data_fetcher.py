import pandas as pd

def get_sqlserver_databases(conn):
    query = "SELECT name FROM sys.databases WHERE database_id > 4"
    return pd.read_sql(query, conn)['name'].tolist()

def get_sqlserver_schemas(conn):
    query = """ SELECT s.name, p.name AS owner_name, p.type_desc
            FROM sys.schemas s
            LEFT JOIN sys.database_principals p ON s.principal_id = p.principal_id
            WHERE p.name = 'dbo' AND s.name != 'dbo'
            ORDER BY s.name
            """
    return pd.read_sql(query, conn)['name'].tolist()

def get_table_list(conn, source='sqlserver', schema='EXL_SCHEMA'):
    if source == 'sqlserver':
        query = f"""
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '{schema}' ORDER BY TABLE_NAME
        """
        return pd.read_sql(query, conn)['TABLE_NAME'].tolist()
    elif source == 'snowflake':
        query = f"SHOW TABLES IN SCHEMA {schema}"
        return pd.read_sql(query, conn)['name'].tolist()

def get_table_row_count(conn, table_name, source='sqlserver', schema='EXL_SCHEMA'):
    if source in ['sqlserver', 'snowflake']:
        query = f'SELECT COUNT(*) AS "count" FROM {schema}.{table_name}'
        return pd.read_sql(query, conn)['count'].iloc[0]
    else:
        raise ValueError("Unsupported source. Use 'sqlserver' or 'snowflake'.")

def get_table_schema(conn, table_name, source='sqlserver', schema='EXL_SCHEMA'):
    if source == 'sqlserver':
        query = f"""
        SELECT UPPER(COLUMN_NAME) AS COLUMN_NAME, UPPER(DATA_TYPE) AS DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'
        ORDER BY COLUMN_NAME
        """
    elif source == 'snowflake':
        query = f"""
        SELECT UPPER(COLUMN_NAME) AS COLUMN_NAME, UPPER(DATA_TYPE) AS DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema.upper()}' AND TABLE_NAME = '{table_name.upper()}'
        ORDER BY COLUMN_NAME
        """
    else:
        raise ValueError("Unsupported source type.")

    return pd.read_sql(query, conn)

def get_sample_data(conn, table_name, n=100, source='sqlserver', schema='EXL_SCHEMA'):
    if source == 'sqlserver':
        query = f"SELECT TOP {n} * FROM {schema}.{table_name}"
    elif source == 'snowflake':
        query = f"SELECT * FROM {schema}.{table_name} LIMIT {n}"
    return pd.read_sql(query, conn)

# scripts/data_fetcher.py

def get_snowflake_databases(conn):
    query = "SHOW DATABASES"
    df = pd.read_sql(query, conn)
    return df['name'].tolist()

def get_snowflake_schemas(conn):
    query = "SHOW SCHEMAS"
    df = pd.read_sql(query, conn)
    return df['name'].tolist()
