"""
Streamlit page for importing marketplace reports into the database.

This page allows users to:
- Select a marketplace (Ozon or Wildberries).
- Provide report files (or paths to files/folders) corresponding to the selected marketplace.
  - Users can upload files directly or use pre-configured paths from `config.json`.
- Trigger an import process that reads the files, transforms data according to `mp_reports_schema.md`,
  and loads it into the DuckDB database.
- View feedback on the success or failure of each import operation.
"""
import streamlit as st
import pandas as pd
import os
from utils.config_utils import load_config, get_report_path, get_db_path
from utils.db_connection import connect_db, get_connection_and_ensure_schema
from utils.db_crud import import_data_from_dataframe
from utils.db_schema import get_table_schema_definition, get_defined_table_names, create_tables_from_schema
from utils.google_sheets_utils import read_google_sheets_as_dataframe, validate_google_sheets_url, test_google_sheets_access

# (Keep other imports like ui_utils if they exist)
# from utils.ui_utils import show_navigation_links

# Initialize session state for selected marketplace if not already done
if 'selected_marketplace' not in st.session_state:
    st.session_state.selected_marketplace = "Ozon" # Default selection

# Load configuration
config = load_config()
# Ensure db_path is loaded correctly for use in this script if needed for connection status
db_path = get_db_path()

st.set_page_config(layout="wide", page_title="Импорт отчетов маркетплейсов")
# show_navigation_links() # If using ui_utils
st.title("🛒 Импорт отчетов маркетплейсов")
st.write("На этой странице вы можете импортировать различные отчеты с маркетплейсов Ozon и Wildberries.")

# --- Helper Function to get connection and ensure schema ---
db_conn = get_connection_and_ensure_schema()

if not db_conn:
    st.warning("Нет активного соединения с базой данных. Импорт невозможен.")
    st.error("Путь к базе данных не настроен или не удалось подключиться. Пожалуйста, проверьте настройки.")
    if st.button("Перейти в Настройки для конфигурации БД"):
        st.switch_page("pages/3_Settings.py")
    st.stop()
else:
    # Check if schema creation was successful
    if not create_tables_from_schema(db_conn):
        st.warning("Ошибка при создании/проверке таблиц в БД. Импорт может работать некорректно.")
    
    # Check if database migration is needed
    from utils.db_migration import auto_migrate_if_needed
    if not auto_migrate_if_needed(db_conn):
        st.warning("⚠️ Требуется выполнить миграцию базы данных перед импортом.")
        st.info("Выполните миграцию выше, затем обновите страницу для продолжения импорта.")
        st.stop()

# --- Marketplace and Report Selection ---
st.sidebar.title("Выбор маркетплейса")
mp_options = ["Ozon", "Wildberries"]
st.session_state.selected_marketplace = st.sidebar.radio(
    "Выберите маркетплейс:",
    mp_options,
    index=mp_options.index(st.session_state.selected_marketplace) # Maintain selection
)

# Get defined tables for the selected marketplace from schema
defined_tables = get_defined_table_names() # This gets ALL table names
# We need to filter them by marketplace, or adjust schema to include marketplace info
# For now, let's assume get_table_schema_definition gives us enough info to filter.

marketplace_specific_tables = {}
for table_name in defined_tables:
    schema_def = get_table_schema_definition(table_name)
    if schema_def:
        # Infer marketplace from table name prefix or a new 'marketplace' key in schema
        # Simple inference for now:
        if st.session_state.selected_marketplace == "Ozon" and table_name.startswith("oz_"):
            marketplace_specific_tables[table_name] = schema_def.get("description", table_name)
        elif st.session_state.selected_marketplace == "Wildberries" and table_name.startswith("wb_"):
            marketplace_specific_tables[table_name] = schema_def.get("description", table_name)

if not marketplace_specific_tables:
    st.sidebar.warning(f"Для {st.session_state.selected_marketplace} не найдено определенных отчетов для импорта.")
else:
    st.sidebar.subheader(f"Отчеты для {st.session_state.selected_marketplace}")
    selected_report_key = st.sidebar.selectbox(
        "Выберите тип отчета для импорта:",
        options=list(marketplace_specific_tables.keys()),
        format_func=lambda x: marketplace_specific_tables[x]
    )
    
    st.subheader(f"Импорт: {marketplace_specific_tables.get(selected_report_key, '')} ({st.session_state.selected_marketplace})")

    schema_for_selected_report = get_table_schema_definition(selected_report_key)
    
    if schema_for_selected_report:
        config_key_for_path = schema_for_selected_report.get("config_report_key")
        default_path = get_report_path(config_key_for_path) if config_key_for_path else ""
        file_type = schema_for_selected_report.get("file_type", "file") # "file" or "folder_xlsx" or "google_sheets"
        
        uploaded_files = None # Initialize to None
        data_source_path = None # Initialize
        use_config_path = False

        if file_type == "google_sheets":
            st.info("Для этого типа отчета будет использована ссылка на Google Sheets из настроек.")
            sheets_url = default_path
            
            if sheets_url:
                if validate_google_sheets_url(sheets_url):
                    st.success(f"✅ Google Sheets URL настроен: {sheets_url[:50]}...")
                    if st.button("🔗 Тестировать доступ к Google Sheets", key=f"test_sheets_{selected_report_key}"):
                        with st.spinner("Проверка доступа к Google Sheets..."):
                            if test_google_sheets_access(sheets_url):
                                st.success("✅ Google Sheets документ доступен для импорта")
                            else:
                                st.error("❌ Google Sheets документ недоступен. Проверьте ссылку и права доступа.")
                else:
                    st.error("❌ Некорректная ссылка на Google Sheets. Проверьте URL в настройках.")
            else:
                st.warning("⚠️ Google Sheets URL не настроен. Перейдите в настройки для конфигурации.")
                if st.button("Перейти в Настройки", key=f"goto_settings_{selected_report_key}"):
                    st.switch_page("pages/3_Settings.py")
            
            data_source_path = sheets_url
            
        elif file_type == "folder_xlsx":
            st.info("Для этого типа отчета ожидается папка с XLSX файлами.")
            folder_path_input = st.text_input("Путь к папке с файлами XLSX:", value=default_path, key=f"folder_path_{selected_report_key}")
            if folder_path_input and os.path.isdir(folder_path_input):
                try:
                    if not any(f.endswith(".xlsx") for f in os.listdir(folder_path_input)):
                        st.warning("В указанной папке нет XLSX файлов.")
                except Exception as e:
                    st.error(f"Не удалось прочитать папку: {e}")
            elif folder_path_input: # Path provided but not a directory
                 st.warning("Указанный путь не является папкой или недоступен.")
            data_source_path = folder_path_input
            
        else: # Single file upload
            accept_multiple = schema_for_selected_report.get("accept_multiple_files", False) # Get from schema or default to False
            file_label = f"Загрузите файл отчета ({schema_for_selected_report.get('file_type_description', schema_for_selected_report.get('file_type', ''))})" # Improved label

            if default_path and os.path.exists(default_path): # Check if default_path is valid and exists
                use_config_path = st.checkbox(
                    f"Использовать путь из настроек: {default_path}", 
                    value=True, # Default to using config path if available
                    key=f"use_config_path_{selected_report_key}"
                )
            
            if use_config_path and default_path:
                data_source_path = default_path
                st.caption(f"Будет использован файл: {default_path}")
                # Optionally disable or hide file_uploader when using config path
                # For simplicity, we'll just ignore uploaded_files if use_config_path is True
            else:
                uploaded_files = st.file_uploader(
                    file_label,
                    type=[schema_for_selected_report.get("file_type")] if schema_for_selected_report.get("file_type") not in ["csv", "xlsx"] else ["csv", "xlsx"],
                    accept_multiple_files=accept_multiple,
                    key=f"uploader_{selected_report_key}"
                )

        if st.button(f"Импортировать {marketplace_specific_tables.get(selected_report_key, '')}"):
            proceed_with_import = False
            if file_type == "google_sheets":
                if data_source_path and validate_google_sheets_url(data_source_path):
                    proceed_with_import = True
                else:
                    st.warning("Некорректная или отсутствующая ссылка на Google Sheets.")
            elif file_type == "folder_xlsx":
                if data_source_path and os.path.isdir(data_source_path):
                    proceed_with_import = True
                else:
                    st.warning("Укажите корректную папку с файлами XLSX.")
            elif use_config_path and data_source_path and os.path.isfile(data_source_path): # Check if file exists
                proceed_with_import = True
            elif not use_config_path and uploaded_files:
                proceed_with_import = True
            else:
                st.warning("Пожалуйста, выберите файл(ы) для импорта или укажите корректный путь.")

            if proceed_with_import:
                with st.spinner(f"Импорт данных..."):
                    all_dfs = []
                    raw_read_params = schema_for_selected_report.get("read_params", {})
                    
                    # Create a mutable copy of read_params for pandas
                    pd_read_params = raw_read_params.copy()
                    
                    # Extract custom parameters and remove them from what's passed to pandas
                    skip_rows_after_header_count = pd_read_params.pop('skip_rows_after_header', 0)
                    pd_read_params.pop('data_starts_on_row', None) # Remove old param if present

                    if file_type == "google_sheets":
                        try:
                            current_df = read_google_sheets_as_dataframe(data_source_path)
                            if current_df is not None:
                                all_dfs.append(current_df)
                            else:
                                st.error("Не удалось загрузить данные из Google Sheets.")
                        except Exception as e_sheets:
                            st.error(f"Ошибка при чтении Google Sheets: {e_sheets}")
                    
                    elif file_type == "folder_xlsx":
                        try:
                            xlsx_files_found = False
                            for f_name in os.listdir(data_source_path):
                                if f_name.endswith(".xlsx"):
                                    xlsx_files_found = True
                                    file_path = os.path.join(data_source_path, f_name)
                                    try:
                                        current_df = pd.read_excel(file_path, engine='openpyxl', **pd_read_params)
                                        if skip_rows_after_header_count > 0:
                                            current_df = current_df.iloc[skip_rows_after_header_count:].reset_index(drop=True)
                                        all_dfs.append(current_df)
                                    except Exception as e_read:
                                        st.error(f"Ошибка при чтении файла {f_name}: {e_read}")
                            if not xlsx_files_found:
                                st.warning(f"Не найдено XLSX файлов в папке {data_source_path}.")        
                            elif not all_dfs and xlsx_files_found : # Files were there, but all failed to read or were empty
                                st.warning(f"Не удалось прочитать данные из XLSX файлов в папке {data_source_path}, либо файлы пусты.")

                        except Exception as e_folder:
                             st.error(f"Ошибка при доступе к папке {data_source_path}: {e_folder}")
                    
                    elif use_config_path and data_source_path: # Using path from config
                        try:
                            if schema_for_selected_report.get("file_type") == "csv":
                                current_df = pd.read_csv(data_source_path, **pd_read_params)
                                # CSVs unlikely to have skip_rows_after_header, but if schema allows, it would apply here too
                                # However, for CSV, skiprows parameter is more direct if needed.
                                # For now, assuming skip_rows_after_header is excel-specific logic in this context.
                            elif schema_for_selected_report.get("file_type") == "xlsx":
                                # Add safety check for large integers in Excel files
                                safe_read_params = pd_read_params.copy()
                                
                                # If no dtype specified, add default string types for known problematic columns
                                if 'dtype' not in safe_read_params:
                                    safe_read_params['dtype'] = {}
                                
                                # Add string types for columns that might contain large integers
                                if selected_report_key == "oz_barcodes":
                                    safe_read_params['dtype'].update({
                                        'Артикул': 'str',
                                        'Ozon Product ID': 'str',
                                        'Штрихкод': 'str'
                                    })
                                elif "oz_" in selected_report_key:
                                    # For other Ozon tables, protect SKU and Product ID columns
                                    safe_read_params['dtype'].update({
                                        'OZON id': 'str',
                                        'SKU': 'str',
                                        'Ozon Product ID': 'str'
                                    })
                                elif "wb_" in selected_report_key:
                                    # For Wildberries tables, protect SKU columns
                                    safe_read_params['dtype'].update({
                                        'Артикул WB': 'str'
                                    })
                                
                                st.info(f"📖 Чтение файла с защитой от больших чисел: {os.path.basename(data_source_path)}")
                                current_df = pd.read_excel(data_source_path, engine='openpyxl', **safe_read_params)
                                
                                if skip_rows_after_header_count > 0:
                                    current_df = current_df.iloc[skip_rows_after_header_count:].reset_index(drop=True)
                                    
                                st.success(f"✅ Файл успешно прочитан: {len(current_df)} строк")
                            else:
                                st.error(f"Неподдерживаемый тип файла для чтения: {schema_for_selected_report.get('file_type')}")
                                current_df = None # Ensure it's None
                            if current_df is not None:
                                all_dfs.append(current_df)
                        except FileNotFoundError:
                            st.error(f"Файл не найден по указанному пути: {data_source_path}")
                        except Exception as e_read:
                            st.error(f"Ошибка при чтении файла {os.path.basename(data_source_path)}: {e_read}")
                            st.error(f"Подробности ошибки: {str(e_read)}")
                            # Try to provide helpful suggestions
                            if "Value out of range" in str(e_read) or "overflow" in str(e_read).lower():
                                st.warning("💡 Возможная причина: файл содержит очень большие числа. Попробуйте:")
                                st.write("1. Убедитесь, что в схеме настроены правильные типы данных")
                                st.write("2. Проверьте, что все SKU и Product ID колонки читаются как строки")
                                st.write("3. Обратитесь к документации по BIGINT миграции")

                    else: # Single file upload (or list if accept_multiple_files was True)
                        files_to_process = uploaded_files
                        if files_to_process and not isinstance(files_to_process, list):
                            files_to_process = [files_to_process] # Make it a list for uniform processing
                        
                        if files_to_process:
                            for uploaded_file_obj in files_to_process: # Renamed to avoid conflict
                                if uploaded_file_obj is not None:
                                    try:
                                        if schema_for_selected_report.get("file_type") == "csv":
                                            current_df = pd.read_csv(uploaded_file_obj, **pd_read_params)
                                            # Similar to above, skip_rows_after_header is assumed excel-specific here
                                        elif schema_for_selected_report.get("file_type") == "xlsx":
                                            # Add safety check for large integers in uploaded Excel files
                                            safe_read_params = pd_read_params.copy()
                                            
                                            # If no dtype specified, add default string types for known problematic columns
                                            if 'dtype' not in safe_read_params:
                                                safe_read_params['dtype'] = {}
                                            
                                            # Add string types for columns that might contain large integers
                                            if selected_report_key == "oz_barcodes":
                                                safe_read_params['dtype'].update({
                                                    'Артикул': 'str',
                                                    'Ozon Product ID': 'str',
                                                    'Штрихкод': 'str'
                                                })
                                            elif "oz_" in selected_report_key:
                                                # For other Ozon tables, protect SKU and Product ID columns
                                                safe_read_params['dtype'].update({
                                                    'OZON id': 'str',
                                                    'SKU': 'str',
                                                    'Ozon Product ID': 'str'
                                                })
                                            elif "wb_" in selected_report_key:
                                                # For Wildberries tables, protect SKU columns
                                                safe_read_params['dtype'].update({
                                                    'Артикул WB': 'str'
                                                })
                                            
                                            st.info(f"📖 Чтение загруженного файла с защитой от больших чисел: {uploaded_file_obj.name}")
                                            current_df = pd.read_excel(uploaded_file_obj, engine='openpyxl', **safe_read_params)
                                            
                                            if skip_rows_after_header_count > 0:
                                                current_df = current_df.iloc[skip_rows_after_header_count:].reset_index(drop=True)
                                                
                                            st.success(f"✅ Файл успешно прочитан: {len(current_df)} строк")
                                        else: 
                                            st.error(f"Неподдерживаемый тип файла для чтения: {schema_for_selected_report.get('file_type')}")
                                            continue
                                        all_dfs.append(current_df)
                                    except Exception as e_read:
                                        st.error(f"Ошибка при чтении файла {uploaded_file_obj.name}: {e_read}")
                                        st.error(f"Подробности ошибки: {str(e_read)}")
                                        # Try to provide helpful suggestions
                                        if "Value out of range" in str(e_read) or "overflow" in str(e_read).lower():
                                            st.warning("💡 Возможная причина: файл содержит очень большие числа. Попробуйте:")
                                            st.write("1. Убедитесь, что в схеме настроены правильные типы данных")
                                            st.write("2. Проверьте, что все SKU и Product ID колонки читаются как строки")
                                            st.write("3. Обратитесь к документации по BIGINT миграции")
                        else: # This case should be caught by proceed_with_import logic, but as a fallback
                            st.warning("Файлы не были загружены.")
                    
                    if all_dfs:
                        df_combined = pd.concat(all_dfs, ignore_index=True)
                        if not df_combined.empty:
                            target_table_name = selected_report_key # The key is the table name
                            
                            st.write(f"Предпросмотр первых 5 строк для импорта в таблицу '{target_table_name}':")
                            st.dataframe(df_combined.head())

                            success, count, error_message = import_data_from_dataframe(
                                db_conn,
                                df_combined,
                                target_table_name
                            )
                            if success:
                                st.success(f"Успешно импортировано {count} записей в таблицу '{target_table_name}'.")
                                # Optionally, clear uploader or refresh stats
                            else:
                                st.error(f"Ошибка импорта в таблицу '{target_table_name}': {error_message}")
                        else:
                            st.warning("Нет данных для импорта после обработки файлов.")
                    else:
                        st.warning("Не выбраны файлы для импорта или файлы не содержат данных.")
            else:
                st.warning("Пожалуйста, выберите файл(ы) для импорта или укажите корректный путь.")

        # Show special information for punta_table
        if selected_report_key == "punta_table":
            st.info("""
            🔄 **Универсальная таблица punta_table**
            
            Эта таблица автоматически адаптируется к структуре вашего Google Sheets документа:
            • **Динамическая схема**: Автоматически создает колонки на основе заголовков в Google Sheets
            • **Гибкость**: Можете добавлять или изменять колонки в Google Sheets без изменения кода
            • **Автоматическая обработка wb_sku**: Конвертирует wb_sku в числовой формат для совместимости
            • **Полная пересборка**: При каждом импорте таблица полностью пересоздается с новой структурой
            
            **Рекомендации:**
            - Первая строка в Google Sheets должна содержать заголовки колонок
            - Колонка 'wb_sku' будет автоматически обработана как ключевое поле
            - Все остальные колонки будут доступны в аналитических отчетах как PUNTA_название_колонки
            """)
    else:
        st.error(f"Не найдено определение схемы для отчета: {selected_report_key}")

st.sidebar.markdown("---")
if st.sidebar.button("Обновить список отчетов"):
    st.rerun()

# Add any other UI elements or logic as needed
# Example: Displaying last import status or logs
# with st.expander("Логи последнего импорта"):
#    st.json(st.session_state.get("last_import_logs", {})) 