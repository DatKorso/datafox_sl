import duckdb
import streamlit as st
import os
import pandas as pd

# Import from other new utility modules
from .db_schema import get_table_schema_definition, get_table_columns_from_schema, get_defined_table_names
from .config_utils import get_db_path # For get_db_stats
from . import config_utils # For brand filtering
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
    
    Special handling for punta_table: uses dynamic schema creation.
    (Formerly _import_data_from_dataframe in db_utils.py)
    """
    if not con:
        return False, 0, "No database connection."
    if df.empty:
        return True, 0, "Input DataFrame is empty. Nothing to import."
    
    # Special handling for punta_table - use dynamic import
    if table_name == "punta_table":
        return import_dynamic_punta_table(con, df)
    
    table_schema_def = get_table_schema_definition(table_name)
    if not table_schema_def:
        return False, 0, f"No schema definition found for table '{table_name}' via db_schema.py."

    # Check if table uses dynamic schema
    columns_info = table_schema_def.get("columns")
    if columns_info == "DYNAMIC":
        return False, 0, f"Table '{table_name}' uses dynamic schema but special handling is not implemented. Please add specific logic."

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

    # 0.7. Apply brand filter for oz_category_products
    cleaned_df = apply_brand_filter(cleaned_df, table_name)
    
    # Check if any data remains after filtering
    if cleaned_df.empty:
        return False, 0, f"No data remains after applying brand filter for table '{table_name}'. Check your brand filter settings."

    # 0.8. Ensure table exists before pre-update action
    table_exists_query = f"""
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_name = '{table_name}' AND table_schema = 'main'
    """
    
    try:
        table_exists = con.execute(table_exists_query).fetchone()[0] > 0
        
        if not table_exists:
            st.info(f"📋 Таблица '{table_name}' не существует, создаем её...")
            
            # Create table based on schema definition
            columns_definitions = []
            for target_col, sql_type, source_col, notes in schema_columns_info:
                columns_definitions.append(f'"{target_col}" {sql_type}')
            
            if columns_definitions:
                create_table_sql = f"CREATE TABLE \"{table_name}\" ({', '.join(columns_definitions)});"
                
                try:
                    con.execute(create_table_sql)
                    st.success(f"✅ Таблица '{table_name}' успешно создана")
                except Exception as e_create:
                    return False, 0, f"Error creating table '{table_name}': {e_create}. SQL: {create_table_sql}"
            else:
                return False, 0, f"No column definitions found for table '{table_name}'"
        
    except Exception as e_check:
        return False, 0, f"Error checking table existence for '{table_name}': {e_check}"

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
                elif notes == "convert_to_bigint":
                    try:
                        # Convert string to BIGINT (for large SKU/Product ID values)
                        # First, clean the data - remove any non-numeric characters except digits
                        cleaned_series = df_to_import[target_col].astype(str).str.replace(r'[^\d]', '', regex=True)
                        # Convert empty strings to NaN
                        cleaned_series = cleaned_series.replace('', pd.NA)
                        # Convert to numeric, handling large integers
                        numeric_col = pd.to_numeric(cleaned_series, errors='coerce')
                        df_to_import[target_col] = numeric_col.astype('Int64') # Use nullable Int64 for BIGINT
                        
                        # Count and log conversion issues
                        null_count = df_to_import[target_col].isna().sum()
                        if null_count > 0:
                            st.warning(f"Внимание: {null_count} значений в колонке {target_col} не удалось конвертировать в BIGINT и были заменены на NULL")
                        else:
                            st.info(f"✅ Успешно конвертировано {len(df_to_import)} значений в колонке {target_col} в BIGINT")
                    except Exception as e_conv:
                        st.error(f"Ошибка конвертации колонки {target_col} в BIGINT для таблицы {table_name}. Error: {e_conv}. Оставляем как есть.")
                        # Fallback - try basic numeric conversion
                        try:
                            numeric_col = pd.to_numeric(df_to_import[target_col], errors='coerce')
                            df_to_import[target_col] = numeric_col.astype('Int64')
                        except:
                            pass  # Keep original data if all conversions fail
            else:
                # Column is missing in input data - create it as NULL column
                df_to_import[target_col] = pd.NA
    
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

    # 4. Import data into DuckDB table (with MotherDuck-friendly chunking)
    try:
        total_rows = len(df_to_import)
        # Detect if connected to MotherDuck (main database path starts with md:)
        is_motherduck = False
        try:
            db_info = con.execute("SELECT path FROM duckdb_databases() WHERE name = 'main'").fetchone()
            if db_info and isinstance(db_info[0], str) and db_info[0].startswith('md:'):
                is_motherduck = True
        except Exception:
            # Fallback: if detection fails, assume local
            is_motherduck = False

        # Decide chunk size: for MD or large datasets, use chunks to avoid long leases
        if is_motherduck or total_rows > 100_000:
            chunk_size = 50_000 if total_rows > 50_000 else 25_000
            st.info(f"🚚 Импорт с пакетированием: {total_rows} строк, размер пакета {chunk_size}")
            progress_bar = st.progress(0.0)
            imported = 0
            batch_idx = 0
            while imported < total_rows:
                batch_idx += 1
                end = min(imported + chunk_size, total_rows)
                chunk = df_to_import.iloc[imported:end].copy()
                try:
                    con.register('temp_df_to_import', chunk)
                    con.execute(f'INSERT INTO "{table_name}" SELECT * FROM temp_df_to_import;')
                    con.unregister('temp_df_to_import')
                    imported = end
                    progress_bar.progress(imported / total_rows)
                    # Keep-alive ping between batches for long-running cloud sessions
                    try:
                        con.execute('SELECT 1;')
                    except Exception:
                        pass
                except Exception as batch_err:
                    return False, imported, f"Error importing data into table '{table_name}' on batch {batch_idx}: {batch_err}"

            records_imported = imported
        else:
            # Single-shot import for small/local datasets
            con.register('temp_df_to_import', df_to_import)
            con.execute(f'INSERT INTO "{table_name}" SELECT * FROM temp_df_to_import;')
            con.unregister('temp_df_to_import')
            records_imported = total_rows

        # 5. Recreate indexes for this table after successful import
        try:
            from .db_indexing import recreate_indexes_after_import
            recreate_indexes_after_import(con, table_name, silent=False)
        except ImportError:
            # Модуль индексирования недоступен - не критично
            pass
        except Exception as e_index:
            # Ошибка создания индексов не должна прерывать успешный импорт
            st.warning(f"⚠️ Данные импортированы успешно, но не удалось создать индексы: {e_index}")

        return True, records_imported, ""
    except Exception as e_import:
        return False, 0, f"Error importing data into table '{table_name}': {e_import}"

def import_dynamic_punta_table(
    con: duckdb.DuckDBPyConnection,
    df: pd.DataFrame
) -> tuple[bool, int, str]:
    """
    Динамически импортирует данные в таблицу punta_table с автоматическим созданием схемы.
    Использует DuckDB функцию автоматического вывода типов данных.
    Специальная обработка для wb_sku - конвертация в INTEGER, если возможно.
    """
    if not con:
        return False, 0, "No database connection."
    if df.empty:
        return True, 0, "Input DataFrame is empty. Nothing to import."
    
    try:
        # 1. Очистка данных - удаляем полностью пустые строки
        df_clean = df.dropna(how='all').copy()
        
        if df_clean.empty:
            return True, 0, "All rows were empty after cleaning."
        
        st.info(f"📊 Очищено данных: {len(df)} → {len(df_clean)} строк")
        
        # 2. Специальная обработка wb_sku - попытаться конвертировать в INTEGER
        if 'wb_sku' in df_clean.columns:
            st.info("🔄 Специальная обработка wb_sku...")
            original_count = len(df_clean)
            
            # Конвертируем wb_sku в числа, где возможно
            df_clean['wb_sku'] = pd.to_numeric(df_clean['wb_sku'], errors='coerce')
            
            # Удаляем строки с невалидными wb_sku (если wb_sku является ключевым полем)
            df_clean = df_clean.dropna(subset=['wb_sku'])
            df_clean['wb_sku'] = df_clean['wb_sku'].astype('Int64')
            
            invalid_count = original_count - len(df_clean)
            if invalid_count > 0:
                st.warning(f"⚠️ Исключено {invalid_count} строк с невалидными wb_sku")
            
            st.success(f"✅ wb_sku успешно конвертирован в INTEGER для {len(df_clean)} строк")
        
        # 3. Удаляем существующую таблицу
        con.execute("DROP TABLE IF EXISTS punta_table;")
        st.info("🗑️ Существующая таблица punta_table удалена")
        
        # 4. Регистрируем DataFrame во временную таблицу
        con.register('temp_punta_df', df_clean)
        
        # 5. Создаем новую таблицу с автоматическим выводом схемы
        con.execute("""
            CREATE TABLE punta_table AS 
            SELECT * FROM temp_punta_df;
        """)
        
        # 6. Очищаем временную таблицу
        con.unregister('temp_punta_df')
        
        # 7. Показываем информацию о созданной таблице
        schema_info = con.execute("DESCRIBE punta_table;").fetchdf()
        st.success("✅ Таблица punta_table создана с автоматическим выводом схемы:")
        st.dataframe(schema_info, use_container_width=True)
        
        # 8. Показываем превью данных
        preview_data = con.execute("SELECT * FROM punta_table LIMIT 5;").fetchdf()
        st.info("📋 Превью данных в новой таблице:")
        st.dataframe(preview_data, use_container_width=True)
        
        records_imported = len(df_clean)
        
        # 9. Recreate indexes for punta_table after successful import
        try:
            from .db_indexing import recreate_indexes_after_import
            recreate_indexes_after_import(con, "punta_table", silent=False)
        except ImportError:
            # Модуль индексирования недоступен - не критично
            pass
        except Exception as e_index:
            # Ошибка создания индексов не должна прерывать успешный импорт
            st.warning(f"⚠️ Данные импортированы успешно, но не удалось создать индексы: {e_index}")
        
        return True, records_imported, ""
        
    except Exception as e:
        return False, 0, f"Ошибка при динамическом импорте punta_table: {str(e)}"

def get_punta_table_columns(con: duckdb.DuckDBPyConnection) -> list[str]:
    """
    Получает список всех колонок в таблице punta_table (для универсальной работы).
    Возвращает пустой список если таблица не существует.
    """
    if not con:
        return []
    
    try:
        # Проверяем существование таблицы
        table_exists = con.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = 'punta_table' AND table_schema = 'main'
        """).fetchone()[0]
        
        if table_exists == 0:
            return []
        
        # Получаем список колонок
        columns_df = con.execute("DESCRIBE punta_table;").fetchdf()
        return columns_df['column_name'].tolist()
        
    except Exception as e:
        st.warning(f"Не удалось получить колонки таблицы punta_table: {e}")
        return []

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
        'table_record_counts': {},
        'db_size_method': None,
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

        # Determine connection type (local vs MotherDuck)
        is_motherduck = False
        try:
            db_info = con.execute("SELECT path FROM duckdb_databases() WHERE name = 'main'").fetchone()
            if db_info and isinstance(db_info[0], str) and db_info[0].startswith('md:'):
                is_motherduck = True
        except Exception:
            is_motherduck = False

        if is_motherduck:
            # Try precise size via DuckDB storage info (compressed bytes)
            try:
                row = con.execute("SELECT SUM(total_compressed_size) AS bytes FROM duckdb_storage_info()").fetchone()
                if row and row[0] is not None:
                    stats['db_file_size_mb'] = round(float(row[0]) / (1024 * 1024), 2)
                    stats['db_size_method'] = 'md_storage_info'
            except Exception:
                pass

            # Fallback: try PRAGMA database_size (may not be available on MD)
            if stats['db_file_size_mb'] is None:
                try:
                    df = con.execute("PRAGMA database_size").fetchdf()
                    # Heuristic: prefer columns that look like byte counts
                    byte_cols = [c for c in df.columns if 'byte' in c.lower() or 'size' in c.lower()]
                    total_bytes = 0
                    if len(df.index) > 0:
                        for col in byte_cols:
                            try:
                                val = float(df.iloc[0][col])
                                if val > 0:
                                    total_bytes = max(total_bytes, val)
                            except Exception:
                                pass
                    if total_bytes > 0:
                        stats['db_file_size_mb'] = round(total_bytes / (1024 * 1024), 2)
                        stats['db_size_method'] = 'md_database_size_pragma'
                except Exception:
                    pass

            # Final fallback: estimate by schema (row count * avg row width)
            if stats['db_file_size_mb'] is None:
                try:
                    # Prefer managed tables list to limit scope
                    managed = set(get_defined_table_names() or [])
                    if managed:
                        existing = con.execute(
                            """
                            SELECT table_name FROM information_schema.tables 
                            WHERE table_schema = 'main' AND table_name IN (""" + 
                            ",".join([f"'{t}'" for t in managed]) + ")"
                        ).fetchall() or []
                        candidate_tables = [t[0] for t in existing]
                    else:
                        candidate_tables = [t[0] for t in (con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall() or [])]

                    table_rows = {}
                    for tbl in candidate_tables:
                        try:
                            row_cnt = con.execute(f'SELECT COUNT(*) FROM "{tbl}"').fetchone()[0]
                        except Exception:
                            row_cnt = 0
                        table_rows[tbl] = int(row_cnt or 0)

                    # Column type sizes (very rough)
                    def _type_size(t: str) -> int:
                        t = (t or '').upper()
                        if 'BIGINT' in t or 'HUGEINT' in t:
                            return 8
                        if 'INTEGER' in t or 'INT32' in t or t == 'INT':
                            return 4
                        if 'SMALLINT' in t:
                            return 2
                        if 'TINYINT' in t or 'BOOLEAN' in t or 'BOOL' in t:
                            return 1
                        if 'DOUBLE' in t or 'FLOAT8' in t or 'TIMESTAMP' in t:
                            return 8
                        if 'REAL' in t or 'FLOAT' in t:
                            return 4
                        if 'DECIMAL' in t or 'NUMERIC' in t:
                            return 8
                        if 'DATE' in t or 'TIME' in t:
                            return 4
                        if 'UUID' in t:
                            return 16
                        if 'VARCHAR' in t or 'TEXT' in t:
                            # Default avg length for text; refined below if possible
                            return 32
                        # Default fallback
                        return 8

                    total_bytes = 0
                    def qident(name: str) -> str:
                        return '"' + str(name).replace('"', '""') + '"'

                    for tbl, row_cnt in table_rows.items():
                        if row_cnt == 0:
                            continue
                        # Fetch columns and types
                        cols = con.execute(
                            f"""
                            SELECT column_name, data_type
                            FROM information_schema.columns
                            WHERE table_schema='main' AND table_name = '{tbl}'
                            ORDER BY ordinal_position
                            """
                        ).fetchall() or []

                        # Estimate width
                        width = 0
                        text_cols = []
                        for col_name, data_type in cols:
                            sz = _type_size(str(data_type))
                            width += sz
                            if sz == 32:  # text placeholder
                                text_cols.append(col_name)

                        # Try refine text columns average length using a small sample (up to 10k rows)
                        for col in text_cols:
                            try:
                                avg_len_row = con.execute(
                                    f"SELECT AVG(length({qident(col)})) FROM (SELECT {qident(col)} FROM {qident(tbl)} WHERE {qident(col)} IS NOT NULL LIMIT 10000)"
                                ).fetchone()
                                avg_len = float(avg_len_row[0]) if avg_len_row and avg_len_row[0] is not None else 32.0
                                # Replace default 32 with measured avg (cap to 256 to avoid extremes)
                                width += max(0.0, min(256.0, avg_len) - 32.0)
                            except Exception:
                                pass

                        # Add per-row overhead factor (~10%) and indexing/metadata (~10%)
                        row_bytes = width * 1.1
                        total_bytes += int(row_cnt * row_bytes * 1.1)

                    if total_bytes > 0:
                        stats['db_file_size_mb'] = round(total_bytes / (1024 * 1024), 2)
                        stats['db_size_method'] = 'md_schema_estimate'
                except Exception as e_est:
                    print(f"DB size MD estimate failed: {e_est}")

        else:
            # Local file size
            db_path = get_db_path() # From config_utils.py
            if db_path and os.path.exists(db_path):
                try:
                    file_size_bytes = os.path.getsize(db_path)
                    stats['db_file_size_mb'] = round(file_size_bytes / (1024 * 1024), 2)
                    stats['db_size_method'] = 'local_file'
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

def apply_brand_filter(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Применяет фильтр по брендам для таблицы oz_category_products.
    
    Args:
        df: DataFrame для фильтрации
        table_name: Название таблицы
    
    Returns:
        Отфильтрованный DataFrame
    """
    # Применяем фильтр только для таблицы oz_category_products
    if table_name != "oz_category_products":
        return df
    
    # Получаем список брендов из настроек
    brands_filter = config_utils.get_data_filter("oz_category_products_brands")
    
    if not brands_filter or brands_filter.strip() == "":
        st.info("🔍 Фильтр брендов не установлен - загружаются все товары")
        return df
    
    # Разбираем список брендов
    allowed_brands = [brand.strip() for brand in brands_filter.split(";") if brand.strip()]
    
    if not allowed_brands:
        st.info("🔍 Фильтр брендов пустой - загружаются все товары")
        return df
    
    # Ищем колонку с брендом
    brand_columns = [col for col in df.columns if 'бренд' in col.lower() or 'brand' in col.lower()]
    
    if not brand_columns:
        st.warning("⚠️ Колонка с брендом не найдена - фильтр не применен")
        return df
    
    brand_column = brand_columns[0]  # Берем первую найденную колонку
    
    # Применяем фильтр
    original_count = len(df)
    
    # Создаем маску для фильтрации (регистронезависимый поиск)
    mask = df[brand_column].astype(str).str.lower().isin([brand.lower() for brand in allowed_brands])
    filtered_df = df[mask].copy()
    
    filtered_count = len(filtered_df)
    excluded_count = original_count - filtered_count
    
    # Отображаем результаты фильтрации
    if excluded_count > 0:
        st.success(f"🎯 Фильтр брендов применен: {original_count} → {filtered_count} записей")
        st.info(f"📋 Разрешенные бренды: {', '.join(allowed_brands)}")
        st.warning(f"🚫 Исключено записей: {excluded_count}")
        
        # Показываем статистику по брендам в исходных данных
        if not df[brand_column].isna().all():
            brand_stats = df[brand_column].value_counts().head(10)
            st.info("📊 Статистика брендов в исходных данных (топ-10):")
            for brand, count in brand_stats.items():
                status = "✅" if str(brand).lower() in [b.lower() for b in allowed_brands] else "❌"
                st.write(f"  {status} **{brand}**: {count} товаров")
    else:
        st.info(f"🎯 Все {original_count} записей соответствуют фильтру брендов")
    
    return filtered_df


def apply_brand_filter_for_rating(df: pd.DataFrame) -> pd.DataFrame:
    """
    Применяет фильтр по брендам для данных рейтинга карточек (oz_card_rating).
    Фильтрует по колонке 'Бренд' используя настройку oz_category_products_brands.
    
    Args:
        df: DataFrame с данными рейтинга для фильтрации
    
    Returns:
        Отфильтрованный DataFrame
    """
    # Получаем список брендов из настроек
    brands_filter = config_utils.get_data_filter("oz_category_products_brands")
    
    if not brands_filter or brands_filter.strip() == "":
        st.info("🔍 Фильтр брендов не установлен - загружаются рейтинги всех товаров")
        return df
    
    # Разбираем список брендов
    allowed_brands = [brand.strip() for brand in brands_filter.split(";") if brand.strip()]
    
    if not allowed_brands:
        st.info("🔍 Фильтр брендов пустой - загружаются рейтинги всех товаров")
        return df
    
    # Ищем колонку с брендом
    brand_column = None
    for col in df.columns:
        if col.lower() in ['бренд', 'brand']:
            brand_column = col
            break
    
    if not brand_column:
        st.warning("⚠️ Колонка 'Бренд' не найдена в файле рейтингов - фильтр не применен")
        st.info("💡 Убедитесь, что файл содержит колонку 'Бренд' для корректной фильтрации")
        return df
    
    # Применяем фильтр
    original_count = len(df)
    
    # Создаем маску для фильтрации (регистронезависимый поиск)
    mask = df[brand_column].astype(str).str.lower().isin([brand.lower() for brand in allowed_brands])
    filtered_df = df[mask].copy()
    
    filtered_count = len(filtered_df)
    excluded_count = original_count - filtered_count
    
    # Отображаем результаты фильтрации
    if excluded_count > 0:
        st.success(f"🎯 Фильтр брендов для рейтингов применен: {original_count} → {filtered_count} записей")
        st.info(f"📋 Разрешенные бренды: {', '.join(allowed_brands)}")
        st.warning(f"🚫 Исключено записей: {excluded_count}")
        
        # Показываем статистику по брендам в исходных данных
        if not df[brand_column].isna().all():
            brand_stats = df[brand_column].value_counts().head(10)
            st.info("📊 Статистика брендов в исходных данных рейтингов (топ-10):")
            for brand, count in brand_stats.items():
                status = "✅" if str(brand).lower() in [b.lower() for b in allowed_brands] else "❌"
                st.write(f"  {status} **{brand}**: {count} товаров")
    else:
        st.info(f"🎯 Все {original_count} записей с рейтингами соответствуют фильтру брендов")
    
    return filtered_df 

def migrate_oz_card_rating_schema(conn) -> bool:
    """
    Обновляет схему таблицы oz_card_rating для поддержки десятичных рейтингов.
    Изменяет тип колонки rating с INTEGER на DECIMAL(3,2).
    
    Args:
        conn: соединение с БД
        
    Returns:
        True если миграция прошла успешно, False в противном случае
    """
    try:
        # Проверим, существует ли таблица
        table_exists = conn.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = 'oz_card_rating' AND table_schema = 'main'
        """).fetchone()[0] > 0
        
        if not table_exists:
            st.info("ℹ️ Таблица oz_card_rating не существует - будет создана с новой схемой")
            return True
        
        # Проверим текущий тип колонки rating
        column_info = conn.execute("""
            SELECT data_type 
            FROM information_schema.columns 
            WHERE table_name = 'oz_card_rating' 
            AND column_name = 'rating' 
            AND table_schema = 'main'
        """).fetchone()
        
        if column_info and 'DECIMAL' in str(column_info[0]).upper():
            st.info("✅ Таблица oz_card_rating уже использует правильный тип данных для рейтинга")
            return True
        
        # Выполняем миграцию
        st.info("🔄 Обновление схемы таблицы oz_card_rating...")
        
        # Создаем временную таблицу с новой схемой
        conn.execute("""
            CREATE TABLE oz_card_rating_new (
                oz_sku BIGINT,
                oz_vendor_code VARCHAR,
                rating DECIMAL(3,2),
                rev_number INTEGER
            )
        """)
        
        # Копируем данные из старой таблицы (если есть)
        try:
            conn.execute("""
                INSERT INTO oz_card_rating_new (oz_sku, oz_vendor_code, rating, rev_number)
                SELECT oz_sku, oz_vendor_code, CAST(rating AS DECIMAL(3,2)), rev_number
                FROM oz_card_rating
            """)
            st.info("📋 Данные скопированы в новую таблицу")
        except Exception as e:
            st.warning(f"⚠️ Не удалось скопировать данные: {e}")
        
        # Удаляем старую таблицу
        conn.execute("DROP TABLE oz_card_rating")
        
        # Переименовываем новую таблицу
        conn.execute("ALTER TABLE oz_card_rating_new RENAME TO oz_card_rating")
        
        st.success("✅ Схема таблицы oz_card_rating успешно обновлена")
        return True
        
    except Exception as e:
        st.error(f"❌ Ошибка при обновлении схемы: {e}")
        return False 
