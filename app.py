import streamlit as st
import pandas as pd
import plotly.express as px
from io import BytesIO

# Import our modules
from config.sqlserver_config import get_sqlserver_connection
from config.snowflake_config import get_snowflake_connection
from scripts import data_fetcher, comparator, quality_checks

# Title and description
st.title("Data Quality Check App")
st.markdown("This demo app compares table data between the source (SQL Server) and target (Snowflake) databases.")

# Sidebar: Database and Schema Selection
with st.sidebar:
    st.image("Logo1.png", width=120)
    st.markdown("---") 
    st.sidebar.markdown("## 🔍 SQL Server Connection")

    # Step 1: Connect to master DB to fetch all databases
    conn_master = get_sqlserver_connection(database="master")
    db_list = data_fetcher.get_sqlserver_databases(conn_master)
    conn_master.close()

    selected_db = st.selectbox("Select a SQL Server Database", db_list)

    # Snowflake Connection
    try:
        conn_snowflake = get_snowflake_connection(database=selected_db)
    except Exception as e:
        st.warning("Snowflake connection not configured.")

    sf_db_list = data_fetcher.get_snowflake_databases(conn_snowflake)

    if selected_db not in sf_db_list:
        st.warning(f"Selected database '{selected_db}' not found in Snowflake.")
        st.stop()

    # Step 2: Connect to the selected DB
    conn_sqlserver = get_sqlserver_connection(database=selected_db)

    # Step 3: Fetch schemas and tables
    schemas = data_fetcher.get_sqlserver_schemas(conn_sqlserver)
    selected_schema = st.selectbox("Select a Schema", schemas)
    sf_schemas = data_fetcher.get_snowflake_schemas(conn_snowflake)

    if selected_schema not in schemas:
        st.warning(f"Schema '{selected_schema}' not found in the selected database.")
        conn_sqlserver.close()
        st.stop()

    if selected_schema not in sf_schemas:
        st.warning(f"Selected Schema '{selected_schema}' not found in Snowflake.")
        conn_sqlserver.close()
        st.stop()

    tables_sqlserver = data_fetcher.get_table_list(conn_sqlserver, source='sqlserver', schema=selected_schema)
    tables_sqlserver = [tbl.capitalize() for tbl in tables_sqlserver]

    selected_table = st.selectbox("Select a Table", tables_sqlserver)

    if selected_table not in tables_sqlserver:
        st.warning(f"Table '{selected_table}' not found in schema '{selected_schema}'.")
        conn_sqlserver.close()
        st.stop()

    st.markdown("---")

    if conn_snowflake:
        tables_snowflake = data_fetcher.get_table_list(conn_snowflake, source='snowflake', schema=selected_schema)
        tables_snowflake = [tbl.capitalize() for tbl in tables_snowflake]
    else:
        tables_snowflake = []

    if selected_table not in tables_snowflake:
        st.warning(f"Selected table '{selected_table}' not found in Snowflake schema '{selected_schema}'.")
        conn_sqlserver.close()
        if conn_snowflake:
            conn_snowflake.close()
        st.stop()

    # Sidebar button to trigger full summary generation
    if st.sidebar.button("📋 Generate Full Summary Report"):
        st.session_state.generate_summary = True
    else:
        st.session_state.generate_summary = False

# --- Table Comparison Section ---
st.markdown("---")
st.markdown("### 📁 Table Presence Comparison")

sqlserver_only = list(set(tables_sqlserver) - set(tables_snowflake))
snowflake_only = list(set(tables_snowflake) - set(tables_sqlserver))
common_tables = list(set(tables_sqlserver) & set(tables_snowflake))

col1, col2 = st.columns(2)
with col1:
    st.markdown("#### ✅ Common Tables")
    st.write(common_tables)

with col2:
    st.markdown("#### ❌ Tables only in SQL Server")
    st.write(sqlserver_only)
    st.markdown("#### ❌ Tables only in Snowflake")
    st.write(snowflake_only)

# --- Full Summary Section ---
if st.session_state.get("generate_summary", False):
    st.header("📄 Full Schema-Level Summary Report")
    output = BytesIO()
    with st.expander("Table's Summary"):
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            for table in common_tables:
                # st.write(f"Processing: {table}")
                row_source = data_fetcher.get_table_row_count(conn_sqlserver, table, 'sqlserver', selected_schema)
                row_target = data_fetcher.get_table_row_count(conn_snowflake, table, 'snowflake', selected_schema)
                match, count_source, count_target = comparator.compare_row_counts(row_source, row_target)

                schema_source = data_fetcher.get_table_schema(conn_sqlserver, table, 'sqlserver', selected_schema)
                schema_target = data_fetcher.get_table_schema(conn_snowflake, table, 'snowflake', selected_schema)
                column_match = schema_source.shape[0] == schema_target.shape[0]

                sample_source = data_fetcher.get_sample_data(conn_sqlserver, table, 120, 'sqlserver', selected_schema)
                sample_target = data_fetcher.get_sample_data(conn_snowflake, table, 120, 'snowflake', selected_schema)
                dup_sql = quality_checks.check_duplicates(sample_source)
                dup_sf = quality_checks.check_duplicates(sample_target)
                null_sql = quality_checks.check_nulls(sample_source).round(0)
                null_sf = quality_checks.check_nulls(sample_target).round(0)

                null_sql.index = null_sql.index.str.upper()
                null_sf.index = null_sf.index.str.upper()
                null_df = pd.concat([null_sql, null_sf], axis=1).fillna(0)
                null_df.columns = ["SQL Server (%)", "Snowflake (%)"]
                null_df["Difference"] = (null_df["SQL Server (%)"] - null_df["Snowflake (%)"]).abs().astype(int)
                null_df = null_df.astype(int).reset_index().rename(columns={"index": "Column Name"})

                ## View Data
                st.markdown("---") 
                st.subheader(f"📊 Summary Report - {table}")

                sample_source = data_fetcher.get_sample_data(conn_sqlserver, table, n=120, source='sqlserver',schema=selected_schema)
                sample_target = data_fetcher.get_sample_data(conn_snowflake, table, n=120, source='snowflake',schema=selected_schema)
                duplicates_sql = quality_checks.check_duplicates(sample_source)
                duplicates_snowflake = quality_checks.check_duplicates(sample_target)

                summary_data = {
                    "Check": [
                        "Table Presence in Both DBs",
                        "Row Count Match",
                        "Column Count Match",
                        "Duplicate Rows (SQL Server)",
                        "Duplicate Rows (Snowflake)"
                    ],
                    "Result": [
                        "✅ Present in both" if table in common_tables else "❌ Missing in one",
                        "✅ Match" if match else "❌ Mismatch",
                        "✅ Match" if schema_source.shape[0] == schema_target.shape[0] else "❌ Mismatch",
                        f"{duplicates_sql} duplicates",
                        f"{duplicates_snowflake} duplicates"
                    ]
                }
                summary_df = pd.DataFrame(summary_data)
                st.dataframe(summary_df, use_container_width=True)

                st.markdown("##### 🧪 Null Value Comparison (Source vs Target)")
                # Get null percentages and round to 0 decimals
                nulls_sqlserver = quality_checks.check_nulls(sample_source).round(0)
                nulls_snowflake = quality_checks.check_nulls(sample_target).round(0)

                    # Standardize column names to uppercase
                nulls_sqlserver.index = nulls_sqlserver.index.str.upper()
                nulls_snowflake.index = nulls_snowflake.index.str.upper()

                    # Combine and compare
                null_comparison = pd.concat([nulls_sqlserver, nulls_snowflake], axis=1)
                null_comparison.columns = ['SQL Server (%)', 'Snowflake (%)']

                    # Replace NaN with 0 before calculating difference
                null_comparison.fillna(0, inplace=True)

                    # Calculate and cast
                null_comparison['Difference'] = (null_comparison['SQL Server (%)'] - null_comparison['Snowflake (%)']).abs().astype(int)
                null_comparison[['SQL Server (%)', 'Snowflake (%)']] = null_comparison[['SQL Server (%)', 'Snowflake (%)']].astype(int)

                    # Prepare for display
                null_comparison.reset_index(inplace=True)
                null_comparison.rename(columns={'index': 'Column Name'}, inplace=True)

                    # Highlight differences
                def highlight_diff(val):
                    return 'background-color: red' if val > 0 else ''

                # Display with highlighting
                st.dataframe(null_comparison.style.applymap(highlight_diff, subset=['Difference']), use_container_width=True)

                summary_df = pd.DataFrame({
                    "Metric": [
                        "Row Count Source", "Row Count Target", "Row Count Match",
                        "Column Count Match", "Duplicates SQL Server", "Duplicates Snowflake"
                    ],
                    "Value": [
                        count_source, count_target, match, column_match, dup_sql, dup_sf
                    ]
                })
                summary_df.to_excel(writer, sheet_name=table[:31], index=False, startrow=0)
                null_df.to_excel(writer, sheet_name=table[:31], index=False, startrow=10)

    output.seek(0)

    st.markdown("---") 
    st.download_button(
        label="📅 Download Full Summary Report (Excel)",
        data=output,
        file_name=f"{selected_db}_{selected_schema}_summary.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
else:
    st.markdown("---")

    # Proceed with comparison
    st.header(f"Comparison for Table: **{selected_table}**")

    row_count_source = data_fetcher.get_table_row_count(conn_sqlserver, selected_table, source='sqlserver',schema = selected_schema)
    row_count_target = data_fetcher.get_table_row_count(conn_snowflake, selected_table, source='snowflake',schema = selected_schema)
    match, count_source, count_target = comparator.compare_row_counts(row_count_source, row_count_target)

    st.subheader("Row Count Comparison")
    st.write(f"**Source (SQL Server):** {count_source} rows")
    st.write(f"**Target (Snowflake):** {count_target} rows")
    if match:
        st.success("Row counts match!")
    else:
        st.error("Row counts do NOT match!")

    df_counts = pd.DataFrame({"Source": [count_source], "Target": [count_target]}, index=["Row Count"])
    df_counts = df_counts.reset_index().melt(id_vars='index', value_vars=["Source", "Target"], var_name="Database", value_name="Rows")
    fig = px.bar(df_counts, x='Database', y='Rows', color='Database', title="Row Count Comparison")
    st.plotly_chart(fig)

    st.markdown("---")

    st.subheader("Column Count Comparison")
    schema_source = data_fetcher.get_table_schema(conn_sqlserver, selected_table, source='sqlserver',schema=selected_schema)
    schema_target = data_fetcher.get_table_schema(conn_snowflake, selected_table, source='snowflake',schema=selected_schema)
    st.write(f"**Source (SQL Server):** {schema_source.shape[0]} columns")
    st.write(f"**Target (Snowflake):** {schema_target.shape[0]} columns")
    if schema_source.shape[0] == schema_target.shape[0]:
        st.success("Column counts match!")
    else:
        st.error("Column counts do NOT match!")

    st.markdown("**Source Schema (SQL Server):**")
    st.dataframe(schema_source[['COLUMN_NAME', 'DATA_TYPE']])
    st.markdown("**Target Schema (Snowflake):**")
    st.dataframe(schema_target[['COLUMN_NAME','DATA_TYPE']])

    st.markdown("---")

    st.subheader("Sample Data Comparison")
    sample_source = data_fetcher.get_sample_data(conn_sqlserver, selected_table, n=120, source='sqlserver',schema=selected_schema)
    sample_target = data_fetcher.get_sample_data(conn_snowflake, selected_table, n=120, source='snowflake',schema=selected_schema)

    st.markdown("**Source Sample Data (SQL Server):**")
    st.dataframe(sample_source)

    st.markdown("**Target Sample Data (Snowflake):**")
    st.dataframe(sample_target)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Duplicate Row Count (SQL Server):**")
        duplicates_sql = quality_checks.check_duplicates(sample_source)
        st.write(f'🔁 {duplicates_sql}')

    with col2:
        st.markdown("**Duplicate Row Count (Snowflake):**")
        duplicates_snowflake = quality_checks.check_duplicates(sample_target)
        st.write(f'🔁 {duplicates_snowflake}')

    st.markdown("---")
    # Data Quality Checks on source
    st.subheader("Data Quality Checks")

    # SQL Server nulls
    st.markdown("**Null Value Percentage per Column (SQL Server):**")
    nulls_sql = quality_checks.check_nulls(sample_source)
    nulls_df_sql = nulls_sql.reset_index()
    nulls_df_sql.columns = ['Column Name', 'Percentage (%)']
    nulls_df_sql['Column Name'] = nulls_df_sql['Column Name'].str.upper()  # Convert values to uppercase
    st.dataframe(nulls_df_sql, use_container_width=True)

    # Snowflake nulls
    st.markdown("**Null Value Percentage per Column (Snowflake):**")
    nulls_sf = quality_checks.check_nulls(sample_target)
    nulls_df_sf = nulls_sf.reset_index()
    nulls_df_sf.columns = ['Column Name', 'Percentage (%)']
    nulls_df_sf['Column Name'] = nulls_df_sf['Column Name'].str.upper()  # Convert values to uppercase
    st.dataframe(nulls_df_sf, use_container_width=True)

    st.markdown("---") 
    st.subheader(f"📊 Summary Report - {selected_table}")

    summary_data = {
        "Check": [
            "Table Presence in Both DBs",
            "Row Count Match",
            "Column Count Match",
            "Duplicate Rows (SQL Server)",
            "Duplicate Rows (Snowflake)"
        ],
        "Result": [
            "✅ Present in both" if selected_table in common_tables else "❌ Missing in one",
            "✅ Match" if match else "❌ Mismatch",
            "✅ Match" if schema_source.shape[0] == schema_target.shape[0] else "❌ Mismatch",
            f"{duplicates_sql} duplicates",
            f"{duplicates_snowflake} duplicates"
        ]
    }
    summary_df = pd.DataFrame(summary_data)
    st.dataframe(summary_df, use_container_width=True)

    st.markdown("##### 🧪 Null Value Comparison (Source vs Target)")
    # Get null percentages and round to 0 decimals
    nulls_sqlserver = quality_checks.check_nulls(sample_source).round(0)
    nulls_snowflake = quality_checks.check_nulls(sample_target).round(0)

        # Standardize column names to uppercase
    nulls_sqlserver.index = nulls_sqlserver.index.str.upper()
    nulls_snowflake.index = nulls_snowflake.index.str.upper()

        # Combine and compare
    null_comparison = pd.concat([nulls_sqlserver, nulls_snowflake], axis=1)
    null_comparison.columns = ['SQL Server (%)', 'Snowflake (%)']

        # Replace NaN with 0 before calculating difference
    null_comparison.fillna(0, inplace=True)

        # Calculate and cast
    null_comparison['Difference'] = (null_comparison['SQL Server (%)'] - null_comparison['Snowflake (%)']).abs().astype(int)
    null_comparison[['SQL Server (%)', 'Snowflake (%)']] = null_comparison[['SQL Server (%)', 'Snowflake (%)']].astype(int)

        # Prepare for display
    null_comparison.reset_index(inplace=True)
    null_comparison.rename(columns={'index': 'Column Name'}, inplace=True)

        # Highlight differences
    def highlight_diff(val):
        return 'background-color: red' if val > 0 else ''

    # Display with highlighting
    st.dataframe(null_comparison.style.applymap(highlight_diff, subset=['Difference']), use_container_width=True)


# Close connections
conn_sqlserver.close()
if conn_snowflake:
    conn_snowflake.close()