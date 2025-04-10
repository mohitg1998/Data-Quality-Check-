import streamlit as st
import snowflake.connector

def get_snowflake_connection(database=None, schema=None):
    creds = st.secrets["snowflake"]
    return snowflake.connector.connect(
        user=creds["user"],
        password=creds["password"],
        account=creds["account"],
        warehouse=creds["warehouse"],
        database=database or creds["database"],
        schema=schema or creds["schema"]
    )
