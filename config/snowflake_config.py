import streamlit as st
import snowflake.connector

def get_snowflake_connection():
    creds = st.secrets["snowflake"]
    return snowflake.connector.connect(
        user=creds["user"],
        password=creds["password"],
        account=creds["account"],
        warehouse=creds["warehouse"],
        database=creds["database"],
        schema=creds["schema"]
    )
