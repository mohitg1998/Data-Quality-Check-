import pyodbc

def get_sqlserver_connection(database=None):
    """
    Returns a connection to the local SQL Server instance.
    Uses Windows Authentication.
    
    Parameters:
    - database_name (str): Optional. If provided, connects to that specific database.
    """
    server = "MOHIT\SQLEXPRESS"  # Adjust if your instance name is different
    driver = "ODBC Driver 17 for SQL Server"  # Make sure this is installed

    if database:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            "Trusted_Connection=yes;"
        )
    else:
        # Default to master if no DB specified
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            "DATABASE=master;"
            "Trusted_Connection=yes;"
        )

    return pyodbc.connect(conn_str)
