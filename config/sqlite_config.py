import sqlite3
import os

def get_sqlite_connection():
    # Ensure the data folder exists and the DB is at the right path
    db_path = os.path.join(os.path.dirname(__file__), "..", "data", "source_data.sqlite")
    conn = sqlite3.connect(db_path)
    return conn
