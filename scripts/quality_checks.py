import pandas as pd

def check_duplicates(df):
    return df.duplicated().sum()

def check_nulls(df):
    # Returns percentage of nulls per column
    nulls = df.isnull().mean() * 100
    return nulls
