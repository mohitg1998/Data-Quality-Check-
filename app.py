import streamlit as st
import pandas as pd
import plotly.express as px

# Import our modules
from config.sqlite_config import get_sqlite_connection
from config.snowflake_config import get_snowflake_connection
from scripts import data_fetcher, comparator, quality_checks

# Title and description
st.title("Data Quality Check App")
st.markdown("This demo app compares table data between the source (SQLite) and target (Snowflake) databases.")

# Get connections
conn_sqlite = get_sqlite_connection()

# For Snowflake, wrap in try/except to handle missing credentials in demo mode.
try:
    conn_snowflake = get_snowflake_connection()
except Exception as e:
    st.warning("Snowflake connection not configured. Using SQLite as a placeholder for Snowflake data.")
    conn_snowflake = get_sqlite_connection()  # For demo purposes


# Sidebar: Select table to compare
with st.sidebar:
    st.image("Logo1.png", width=120)  # Optional: Add your company logo here
    st.markdown("---") 

    st.sidebar.markdown("## 🔍 Schema Selection")

    # sqlite_schemas = ['main']  # SQLite doesn't really use schemas like Snowflake, but keep it uniform
    snowflake_schemas = data_fetcher.get_snowflake_schemas(conn_snowflake)
    snowflake_schemas.remove('INFORMATION_SCHEMA')

    # selected_sqlite_schema = st.sidebar.selectbox("SQLite Schema", sqlite_schemas)
    selected_snowflake_schema = st.sidebar.selectbox("Schema", snowflake_schemas)

    # Get table lists
    # Fetch table names
    tables_sqlite = data_fetcher.get_table_list(conn_sqlite, source='sqlite')
    tables_snowflake = data_fetcher.get_table_list(conn_snowflake, source='snowflake', schema=selected_snowflake_schema)

    tables_sqlite = [sq.capitalize() for sq in tables_sqlite]
    tables_sqlite.append("Activity")

    tables_snowflake =  [sq.capitalize() for sq in tables_snowflake] 

    if not tables_snowflake:
        tables_sqlite.clear()

    selected_table = st.sidebar.selectbox("Select a Table", tables_sqlite)


st.markdown("---") 

st.markdown("### 📑 Table Presence Comparison")
# Compare
sqlite_only = list(set(tables_sqlite) - set(tables_snowflake))
snowflake_only = list(set(tables_snowflake) - set(tables_sqlite))
common_tables = list(set(tables_sqlite) & set(tables_snowflake))

col1, col2 = st.columns(2)

with col1:
    st.markdown("#### ✅ Common Tables")
    st.write(common_tables)

with col2:
    st.markdown("#### ❌ Tables only in SQLite")
    st.write(sqlite_only)

    st.markdown("#### ❌ Tables only in Snowflake")
    st.write(snowflake_only)

st.markdown("---") 

if tables_sqlite and selected_table in tables_snowflake:

    st.header(f"Comparison for Table: **{selected_table}**")

    # Row counts
    row_count_source = data_fetcher.get_table_row_count(conn_sqlite, selected_table, source='sqlite')
    row_count_target = data_fetcher.get_table_row_count(conn_snowflake, selected_table, source='snowflake')
    match, count_source, count_target = comparator.compare_row_counts(row_count_source, row_count_target)

    st.subheader("Row Count Comparison")
    st.write(f"**Source (SQLite):** {count_source} rows")
    st.write(f"**Target (Snowflake):** {count_target} rows")
    if match:
        st.success("Row counts match!")
    else:
        st.error("Row counts do NOT match!")

    # Display row count chart
    df_counts = pd.DataFrame({
        "Source": [count_source],
        "Target": [count_target]
    }, index=["Row Count"])
    df_counts = df_counts.reset_index().melt(id_vars='index', value_vars=["Source", "Target"], var_name="Database", value_name="Rows")
    fig = px.bar(df_counts, x='Database', y='Rows', color='Database', title="Row Count Comparison")
    st.plotly_chart(fig)

    st.markdown("---") 

    # Schema Comparison
    st.subheader("Schema Comparison")
    schema_source = data_fetcher.get_table_schema(conn_sqlite, selected_table, source='sqlite')
    schema_target = data_fetcher.get_table_schema(conn_snowflake, selected_table, source='snowflake')

    if schema_source['name'].count() == schema_target['COLUMN_NAME'].count():
        st.success("Column counts match!")
    else:
        st.error("Column counts do NOT match!")

    st.markdown("**Source Schema (SQLite):**")
    st.dataframe(schema_source[['name', 'type']])

    st.markdown("**Target Schema (Snowflake):**")
    st.dataframe(schema_target[['COLUMN_NAME','DATA_TYPE']])


    st.markdown("---") 

    # Sample Data Comparison
    st.subheader("Sample Data Comparison")
    sample_source = data_fetcher.get_sample_data(conn_sqlite, selected_table, n=120, source='sqlite')
    sample_target = data_fetcher.get_sample_data(conn_snowflake, selected_table, n=120, source='snowflake')

    st.markdown("**Source Sample Data (SQLite):**")
    st.dataframe(sample_source)

    st.markdown("**Target Sample Data (Snowflake):**")
    st.dataframe(sample_target)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Duplicate Row Count (SQLite):**")
        duplicates_sqlite = quality_checks.check_duplicates(sample_source)
        st.write(duplicates_sqlite)

    with col2:
        st.markdown("**Duplicate Row Count (Snowflake):**")
        duplicates_snowflake = quality_checks.check_duplicates(sample_target)
        st.write(duplicates_snowflake)

    st.markdown("---") 

    # Data Quality Checks on source
    st.subheader("Data Quality Checks (Source)")
    st.markdown("**Null Value Percentage per Column (SQLite):**")
    nulls = quality_checks.check_nulls(sample_source)
    nulls_df = nulls.reset_index()
    nulls_df.columns = ['Column Name', 'Percentage (%)']
    st.dataframe(nulls_df, use_container_width=True)

    st.markdown("**Null Value Percentage per Column (Snowflake):**")
    nulls = quality_checks.check_nulls(sample_target)
    nulls_df = nulls.reset_index()
    nulls_df.columns = ['Column Name', 'Percentage (%)']
    st.dataframe(nulls_df, use_container_width=True)

    st.markdown("---") 
    st.subheader("📊 Summary Report")

    summary_data = {
        "Check": [
            "Table Presence in Both DBs",
            "Row Count Match",
            "Column Count Match",
            "Duplicate Rows (SQLite)",
            "Duplicate Rows (Snowflake)"
        ],
        "Result": [
            "✅ Present in both" if selected_table in common_tables else "❌ Missing in one",
            "✅ Match" if match else "❌ Mismatch",
            "✅ Match" if schema_source['name'].count() == schema_target['COLUMN_NAME'].count() else "❌ Mismatch",
            f"{duplicates_sqlite} duplicates",
            f"{duplicates_snowflake} duplicates"
        ]
    }

    summary_df = pd.DataFrame(summary_data)
    st.dataframe(summary_df, use_container_width=True)


    st.markdown("### 🧪 Null Value Comparison (Source vs Target)")

    # Get null percentages and round to 0 decimals
    nulls_sqlite = quality_checks.check_nulls(sample_source).round(0)
    nulls_snowflake = quality_checks.check_nulls(sample_target).round(0)

    # Standardize column names to uppercase
    nulls_sqlite.index = nulls_sqlite.index.str.upper()
    nulls_snowflake.index = nulls_snowflake.index.str.upper()

    # Combine and compare
    null_comparison = pd.concat([nulls_sqlite, nulls_snowflake], axis=1)
    null_comparison.columns = ['SQLite (%)', 'Snowflake (%)']

    # Replace NaN with 0 before calculating difference
    null_comparison.fillna(0, inplace=True)

    # Calculate and cast
    null_comparison['Difference'] = (null_comparison['SQLite (%)'] - null_comparison['Snowflake (%)']).abs().astype(int)
    null_comparison[['SQLite (%)', 'Snowflake (%)']] = null_comparison[['SQLite (%)', 'Snowflake (%)']].astype(int)

    # Prepare for display
    null_comparison.reset_index(inplace=True)
    null_comparison.rename(columns={'index': 'Column Name'}, inplace=True)

    # Highlight differences
    def highlight_diff(val):
        return 'background-color: orange' if val > 0 else ''

    # Display with highlighting
    st.dataframe(null_comparison.style.applymap(highlight_diff, subset=['Difference']), use_container_width=True)

else:
    if selected_snowflake_schema == 'EXL_SCHEMA':
        st.markdown("---") 
        st.subheader("📊 Summary Report")

        summary_data = {
            "Check": [
                "Table Presence in Both DBs",
            ],
            "Result": [
                "✅ Present in both" if selected_table in common_tables else "❌ Missing in one",
            ]
        }

        summary_df = pd.DataFrame(summary_data)
        st.dataframe(summary_df, use_container_width=True)


# Close connections (optional, since script ends after execution)
conn_sqlite.close()
conn_snowflake.close()
