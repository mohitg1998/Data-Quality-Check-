import pandas as pd

def check_nulls(df):
    # Returns percentage of nulls per column
    nulls = df.isnull().mean() * 100
    return nulls

def check_duplicates(df):
    # Returns count of duplicate rows
    dup_count = df.duplicated().sum()
    return dup_count

def basic_stats(df):
    # Returns basic statistics for numeric columns
    return df.describe()
