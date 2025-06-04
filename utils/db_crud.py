import duckdb
import streamlit as st
import os
import pandas as pd

# Import from other new utility modules
from .db_schema import get_table_schema_definition, get_table_columns_from_schema, get_defined_table_names
from .config_utils import get_db_path # For get_db_stats
from .data_cleaner import apply_data_cleaning, display_cleaning_report, validate_required_fields

# --- Data Import Functions ---

def import_data_from_dataframe(
    con: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    table_name: str,
) -> tuple[bool, int, str]:
    """
    Imports data from a Pandas DataFrame into a specified DuckDB table according to hardcoded schema.
    Handles pre-update action (e.g., delete all existing data from the table).
    Renames DataFrame columns to match target DuckDB table column names based on schema.
    Applies specific data transformations as noted in the schema.
    Now includes data cleaning and validation with detailed logging.
    (Formerly _import_data_from_dataframe in db_utils.py)
    """
    if not con:
        return False, 0, "No database connection."
    if df.empty:
        return True, 0, "Input DataFrame is empty. Nothing to import."
    
    table_schema_def = get_table_schema_definition(table_name)
    if not table_schema_def:
        return False, 0, f"No schema definition found for table '{table_name}' via db_schema.py."

    schema_columns_info = get_table_columns_from_schema(table_name)
    if not schema_columns_info:
        return False, 0, f"No column schema information found for table '{table_name}' via db_schema.py."

    # 0. Validate required fields
    validation_issues = validate_required_fields(df, schema_columns_info)
    if validation_issues:
        st.error("❌ Проблемы валидации данных:")
        for issue in validation_issues:
            st.write(f"• {issue['message']}")
        return False, 0, "Data validation failed. See details above."

    # 0.5. Apply data cleaning BEFORE other transformations
    st.info("🧹 Очистка и проверка данных...")
    cleaned_df, cleaning_issues = apply_data_cleaning(df, table_name, schema_columns_info)
    
    # Display cleaning report
    display_cleaning_report(cleaning_issues, table_name)

    # 1. Pre-Update Action
    pre_update_sql = table_schema_def.get("pre_update_action")
    if pre_update_sql:
        try:
            con.execute(pre_update_sql)
        except Exception as e:
            return False, 0, f"Error executing pre-update action for table '{table_name}': {e}. SQL: {pre_update_sql}"

    # 2. Prepare DataFrame: Select and rename columns, apply transformations
    df_to_import = pd.DataFrame()
    expected_target_columns = []
    try:
        for target_col, sql_type, source_col, notes in schema_columns_info:
            expected_target_columns.append(target_col)
            if source_col in cleaned_df.columns:
                df_to_import[target_col] = cleaned_df[source_col].copy()

                if notes == "remove_single_quotes":
                    df_to_import[target_col] = df_to_import[target_col].astype(str).str.replace("'", "", regex=False)
                elif notes == "convert to date":
                    try:
                        df_to_import[target_col] = pd.to_datetime(df_to_import[target_col], errors='coerce').dt.date
                    except Exception as e_date:
                        st.warning(f"Could not convert column {target_col} to date for table {table_name}. Error: {e_date}. Leaving as is.")
                elif notes == "round_to_integer":
                    try:
                        # The data should already be cleaned by data_cleaner, but apply final rounding
                        numeric_col = pd.to_numeric(df_to_import[target_col], errors='coerce')
                        df_to_import[target_col] = numeric_col.apply(lambda x: int(round(x)) if pd.notnull(x) else pd.NA)
                        df_to_import[target_col] = df_to_import[target_col].astype('Int64') # Convert to nullable integer type
                    except Exception as e_price:
                        st.warning(f"Could not convert column {target_col} to rounded integer for table {table_name}. Error: {e_price}. Leaving as is.")
                elif notes == "convert_to_integer":
                    try:
                        # Convert string/varchar wb_sku to integer
                        numeric_col = pd.to_numeric(df_to_import[target_col], errors='coerce')
                        df_to_import[target_col] = numeric_col.astype('Int64') # Convert to nullable integer type
                        
                        # Count and log conversion issues
                        null_count = df_to_import[target_col].isna().sum()
                        if null_count > 0:
                            st.warning(f"Внимание: {null_count} значений в колонке {target_col} не удалось конвертировать в числа и были заменены на NULL")
                    except Exception as e_conv:
                        st.warning(f"Could not convert column {target_col} to integer for table {table_name}. Error: {e_conv}. Leaving as is.")
            else:
                return False, 0, f"Source column '{source_col}' defined in schema not found in input data for table '{table_name}'."
    
        for target_col, _, _, _ in schema_columns_info:
            if target_col not in df_to_import.columns:
                df_to_import[target_col] = pd.NA
        
        df_to_import = df_to_import[expected_target_columns]

    except Exception as e_prep:
        return False, 0, f"Error preparing DataFrame for table '{table_name}': {e_prep}"

    # 3. Final check: show preview of data to be imported
    st.info("📊 Предпросмотр данных для импорта:")
    st.dataframe(df_to_import.head(10))
    
    # Show summary statistics
    total_rows = len(df_to_import)
    null_summary = {}
    for col in df_to_import.columns:
        null_count = df_to_import[col].isna().sum()
        if null_count > 0:
            null_summary[col] = null_count
    
    if null_summary:
        st.info("📋 Сводка по пустым значениям:")
        for col, count in null_summary.items():
            st.write(f"• **{col}**: {count} пустых значений из {total_rows} ({count/total_rows*100:.1f}%)")

    # 4. Import data into DuckDB table
    try:
        con.register('temp_df_to_import', df_to_import)
        con.execute(f'INSERT INTO "{table_name}" SELECT * FROM temp_df_to_import;')
        con.unregister('temp_df_to_import')
        
        records_imported = len(df_to_import)
        return True, records_imported, ""
    except Exception as e_import:
        return False, 0, f"Error importing data into table '{table_name}': {e_import}"

# --- Database Statistics ---

def get_db_stats(con: duckdb.DuckDBPyConnection) -> dict:
    """
    Retrieves statistics from the database, such as table count, total records per table,
    and overall total records for managed tables, as well as DB file size.
    """
    if not con:
        return {
            'table_count': None,
            'total_records': None,
            'db_file_size_mb': None,
            'table_record_counts': {},
            'error': 'No database connection.'
        }

    stats = {
        'table_count': 0,
        'total_records': 0,
        'db_file_size_mb': None,
        'table_record_counts': {}
    }

    try:
        table_count_result = con.execute("SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'main';").fetchone()
        stats['table_count'] = table_count_result[0] if table_count_result else 0

        relevant_table_names = get_defined_table_names() # From db_schema.py
        
        total_records_count = 0
        if relevant_table_names:
            for table_name in relevant_table_names:
                try:
                    check_exists = con.execute(f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' AND table_schema = 'main';").fetchone()
                    if check_exists:
                        count_result = con.execute(f'SELECT COUNT(*) FROM "{table_name}";').fetchone()
                        current_table_records = count_result[0] if count_result else 0
                        stats['table_record_counts'][table_name] = current_table_records
                        total_records_count += current_table_records
                    else:
                        stats['table_record_counts'][table_name] = 0
                except Exception as e_count:
                    print(f"Could not get record count for table {table_name}: {e_count}")
                    stats['table_record_counts'][table_name] = f"Error: {e_count}"
            stats['total_records'] = total_records_count
        else:
            stats['total_records'] = None

        db_path = get_db_path() # From config_utils.py
        if db_path and os.path.exists(db_path):
            try:
                file_size_bytes = os.path.getsize(db_path)
                stats['db_file_size_mb'] = round(file_size_bytes / (1024 * 1024), 2)
            except Exception as e_size:
                print(f"Could not get database file size: {e_size}")
                stats['db_file_size_mb'] = f"Error: {e_size}"
        
        return stats

    except Exception as e:
        print(f"Error getting database stats: {e}")
        stats['error'] = str(e)
        if 'table_count' not in stats or stats['table_count'] is None: stats['table_count'] = 0
        if 'total_records' not in stats or stats['total_records'] is None: stats['total_records'] = 0 
        return stats

def get_all_db_tables(con: duckdb.DuckDBPyConnection) -> list[str]:
    """
    Returns a list of ALL table names that exist in the 'main' schema of the database.
    Used for the 'View Data' page to allow viewing any table.
    """
    if not con:
        return []
    
    try:
        tables_result = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name;").fetchall()
        return [row[0] for row in tables_result] if tables_result else []
    except Exception as e:
        msg = f"Error fetching list of all database tables: {e}"
        print(msg)
        if callable(st.error): st.error(msg)
        return [] 