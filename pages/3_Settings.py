"""
Streamlit page for configuring application settings.

This page allows users to:
- Set the path to the DuckDB database file.
- Test the database connection.
- Create a new empty database file and initialize its schema based on `mp_reports_schema.md`.
- Configure paths to various Ozon and Wildberries report files/directories.
- Save all settings to `config.json`.
- Basic validation is performed on file/directory paths upon saving to warn the user if paths are not found,
  though saving is still permitted to allow configuration of paths for files yet to be created/placed.
"""
import streamlit as st
import os
from utils import config_utils
from utils.db_connection import connect_db, test_db_connection, get_connection_and_ensure_schema
from utils.db_schema import create_tables_from_schema

st.set_page_config(page_title="Settings - Marketplace Analyzer", layout="wide")

st.title("⚙️ Settings")
st.markdown("---")

st.info("Configure your database connection and report file paths here. Make sure to save settings after making changes.")

# Load current config
config = config_utils.load_config()

# --- Database Configuration --- 
with st.expander("Database Configuration", expanded=True):
    st.subheader("Database File Setup")
    db_path_current = config_utils.get_db_path()
    db_path_new = st.text_input(
        "Database File Path", 
        value=db_path_current, 
        placeholder="e.g., data/marketplace_data.db",
        help="Path to your DuckDB database file. Relative paths (e.g., data/market.db) are relative to the project root. "
             "If the file doesn't exist where specified, the 'Create Empty Database & Schema' button can make it."
    )

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Test Connection", key="test_db_connection_button", help="Tests if a connection can be established to the database file specified above."):
            if not db_path_new:
                st.error("Database path cannot be empty to test connection.")
            elif test_db_connection(db_path_new):
                st.success(f"Successfully connected to database at: {db_path_new}")
            else:
                st.error(f"Failed to connect to database at: {db_path_new}. Check path and permissions.")
    
    with col2:
        if st.button("Create Empty Database & Schema", 
                      key="create_db_button", 
                      help="Ensures the database file exists at the specified path (creates it if not) and applies the schema from mp_reports_schema.md. Deletes existing tables if they conflict with the schema being applied."):
            if not db_path_new:
                st.error("Database path cannot be empty to create a database.")
            else:
                # Ensure parent directory for the database file exists
                db_dir = os.path.dirname(db_path_new)
                if db_dir and not os.path.exists(db_dir):
                    try:
                        os.makedirs(db_dir, exist_ok=True)
                        st.info(f"Created directory for database: {db_dir}")
                    except Exception as e:
                        st.error(f"Could not create directory {db_dir} for database: {e}")
                        st.stop() # Stop if directory creation fails, as DB connection will likely fail
                
                # Connect to the database (this will create the .db file if it doesn't exist)
                conn = connect_db(db_path_new)
                if conn:
                    st.success(f"Database file ensured/created at: {db_path_new}")
                    # Attempt to create tables based on the schema file
                    if create_tables_from_schema(conn):
                        st.success("Database schema created/verified successfully!")
                    else:
                        st.warning("Attempted to create/verify database schema. Some tables might not have been created or issues were encountered. Check logs.")
                    conn.close() # Close connection after operations
                else:
                    st.error(f"Could not create or connect to database at: {db_path_new}")

# --- Marketplace Report Paths --- 
with st.expander("Marketplace Report Paths"):
    st.subheader("Ozon Report Paths")
    oz_barcodes_current = config_utils.get_report_path("oz_barcodes_xlsx")
    oz_barcodes_new = st.text_input("Ozon Barcodes (.xlsx)", value=oz_barcodes_current, placeholder="Path to oz_barcodes.xlsx", help="Full path to the Ozon barcodes report Excel file.")

    oz_orders_current = config_utils.get_report_path("oz_orders_csv")
    oz_orders_new = st.text_input("Ozon Orders (.csv)", value=oz_orders_current, placeholder="Path to oz_orders.csv", help="Full path to the Ozon orders report CSV file.")

    oz_prices_current = config_utils.get_report_path("oz_prices_xlsx")
    oz_prices_new = st.text_input("Ozon Prices (.xlsx)", value=oz_prices_current, placeholder="Path to oz_prices.xlsx", help="Full path to the Ozon prices report Excel file.")

    oz_products_current = config_utils.get_report_path("oz_products_csv")
    oz_products_new = st.text_input("Ozon Products (.csv)", value=oz_products_current, placeholder="Path to oz_products.csv", help="Full path to the Ozon products report CSV file.")

    # New Ozon folder-based imports
    st.markdown("**Новые папки для импорта продуктов Ozon:**")
    
    oz_category_products_current = config_utils.get_report_path("oz_category_products_folder")
    oz_category_products_new = st.text_input("Ozon Category Products Folder", value=oz_category_products_current, placeholder="Path to folder with category products .xlsx files", help="Путь к папке с XLSX файлами продуктов по категориям Ozon. Все файлы в папке будут обработаны (лист 'Шаблон').")
    
    oz_video_products_current = config_utils.get_report_path("oz_video_products_folder")
    oz_video_products_new = st.text_input("Ozon Video Products Folder", value=oz_video_products_current, placeholder="Path to folder with video products .xlsx files", help="Путь к папке с XLSX файлами видео продуктов Ozon. Все файлы в папке будут обработаны (лист 'Озон.Видео').")
    
    oz_video_cover_products_current = config_utils.get_report_path("oz_video_cover_products_folder")
    oz_video_cover_products_new = st.text_input("Ozon Video Cover Products Folder", value=oz_video_cover_products_current, placeholder="Path to folder with video cover products .xlsx files", help="Путь к папке с XLSX файлами видеообложек продуктов Ozon. Все файлы в папке будут обработаны (лист 'Озон.Видеообложка').")
    
    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("Wildberries Report Paths")
    wb_prices_current = config_utils.get_report_path("wb_prices_xlsx")
    wb_prices_new = st.text_input("Wildberries Prices (.xlsx)", value=wb_prices_current, placeholder="Path to wb_prices.xlsx", help="Full path to the Wildberries prices report Excel file.")

    wb_products_dir_current = config_utils.get_report_path("wb_products_dir")
    wb_products_dir_new = st.text_input("Wildberries Products Directory", value=wb_products_dir_current, placeholder="Path to folder containing wb_products .xlsx files", help="Full path to the folder containing Wildberries products Excel files. All .xlsx files in this folder will be processed.")

    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("Google Sheets Integration")
    punta_sheets_url_current = config_utils.get_report_path("punta_google_sheets_url")
    punta_sheets_url_new = st.text_input("Punta Google Sheets URL", value=punta_sheets_url_current, placeholder="https://docs.google.com/spreadsheets/d/your_sheet_id/edit#gid=0", help="URL ссылка на Google Sheets документ с данными Punta. Документ должен быть доступен для просмотра.")

    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("Custom Reports")
    analytic_report_current = config_utils.get_report_path("analytic_report_xlsx")
    analytic_report_new = st.text_input("Analytic Report (.xlsx)", value=analytic_report_current, placeholder="Path to analytic_report.xlsx", help="Full path to the custom analytic report Excel file. The file should contain 'analytic_report' sheet with proper structure.")

    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("Data Filters")
    oz_brands_current = config_utils.get_data_filter("oz_category_products_brands")
    oz_brands_new = st.text_input(
        "Ozon Category Products - Brands Filter", 
        value=oz_brands_current, 
        placeholder="Shuzzi;Nike;Adidas", 
        help="Указать бренды для загрузки в таблицу oz_category_products. Разделяйте бренды точкой с запятой ';'. Оставьте пустым для загрузки всех брендов."
    )

    if punta_sheets_url_new:
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("🔗 Тестировать Google Sheets", key="test_google_sheets_button"):
                from utils.google_sheets_utils import validate_google_sheets_url, test_google_sheets_access
                
                with st.spinner("Проверка доступа к Google Sheets..."):
                    if not validate_google_sheets_url(punta_sheets_url_new):
                        st.error("❌ Некорректная ссылка на Google Sheets")
                    elif test_google_sheets_access(punta_sheets_url_new):
                        st.success("✅ Google Sheets документ доступен для импорта")
                    else:
                        st.error("❌ Google Sheets документ недоступен. Проверьте ссылку и права доступа.")
        
        with col2:
            if st.button("📋 Предпросмотр данных", key="preview_google_sheets_button"):
                from utils.google_sheets_utils import read_google_sheets_as_dataframe
                
                with st.spinner("Загрузка данных из Google Sheets..."):
                    df = read_google_sheets_as_dataframe(punta_sheets_url_new)
                    if df is not None:
                        st.success(f"✅ Загружено {len(df)} строк")
                        st.dataframe(df.head(), use_container_width=True)
                    else:
                        st.error("❌ Не удалось загрузить данные")
        
        with col3:
            if st.button("🔍 Диагностика кодировки", key="diagnose_encoding_button"):
                from utils.google_sheets_utils import diagnose_google_sheets_encoding
                
                with st.spinner("Диагностика проблем с кодировкой..."):
                    diagnosis = diagnose_google_sheets_encoding(punta_sheets_url_new)
                    
                    if diagnosis['accessible']:
                        st.success("✅ Документ доступен")
                        
                        col_info1, col_info2 = st.columns(2)
                        with col_info1:
                            st.write("**Тип контента:**", diagnosis['content_type'])
                            st.write("**Обнаруженная кодировка:**", diagnosis['encoding_detected'])
                            st.write("**Кириллица найдена:**", "✅ Да" if diagnosis['has_cyrillic'] else "❌ Нет")
                        
                        with col_info2:
                            st.write("**Рекомендации:**")
                            for rec in diagnosis['recommendations']:
                                st.write(f"• {rec}")
                        
                        if diagnosis['sample_content']:
                            st.write("**Образец контента (первые 200 символов):**")
                            st.code(diagnosis['sample_content'], language="text")
                    else:
                        st.error("❌ Документ недоступен для диагностики")

        # Import data button (full width)
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("📥 Загрузить данные в БД (punta_table)", key="import_to_db_button", use_container_width=True):
            from utils.google_sheets_utils import read_google_sheets_as_dataframe
            from utils.db_crud import import_data_from_dataframe
            
            # Get database connection
            try:
                db_conn = get_connection_and_ensure_schema()
                if not db_conn:
                    st.error("❌ Нет соединения с базой данных. Проверьте настройки.")
                else:
                    with st.spinner("Загрузка данных из Google Sheets в базу данных..."):
                        # Read data from Google Sheets
                        df = read_google_sheets_as_dataframe(punta_sheets_url_new)
                        
                        if df is not None:
                            st.info(f"📊 Загружено {len(df)} строк из Google Sheets")
                            
                            # Show preview
                            st.write("**Предпросмотр данных для импорта:**")
                            st.dataframe(df.head(), use_container_width=True)
                            
                            # Import to database
                            success, count, error_message = import_data_from_dataframe(
                                db_conn,
                                df,
                                "punta_table"
                            )
                            
                            if success:
                                st.success(f"✅ Успешно импортировано {count} записей в таблицу 'punta_table'!")
                                st.balloons()
                                
                                # Show some statistics
                                st.info(f"📈 Статистика импорта: {len(df)} строк обработано, {count} записей добавлено в БД")
                            else:
                                st.error(f"❌ Ошибка импорта в таблицу 'punta_table': {error_message}")
                        else:
                            st.error("❌ Не удалось загрузить данные из Google Sheets")
                            
            except Exception as e:
                st.error(f"❌ Ошибка при импорте данных: {e}")
                
# --- Save Settings --- 
st.markdown("---")
if st.button("Save All Settings", key="save_all_settings_button", help="Saves all configured paths below to config.json. Performs a basic check if files/directories exist but allows saving non-existent paths."):
    # Basic path validation before saving. This is a soft validation to warn the user.
    # The application will still save the paths, allowing users to set paths for files they intend to create/place later.
    paths_to_validate = {
        "Ozon Barcodes (.xlsx)": (oz_barcodes_new, False),
        "Ozon Orders (.csv)": (oz_orders_new, False),
        "Ozon Prices (.xlsx)": (oz_prices_new, False),
        "Ozon Products (.csv)": (oz_products_new, False),
        "Ozon Category Products Folder": (oz_category_products_new, True), # Directory
        "Ozon Video Products Folder": (oz_video_products_new, True), # Directory
        "Ozon Video Cover Products Folder": (oz_video_cover_products_new, True), # Directory
        "Wildberries Prices (.xlsx)": (wb_prices_new, False),
        "Wildberries Products Directory": (wb_products_dir_new, True), # True indicates it's a directory
        "Punta Google Sheets URL": (punta_sheets_url_new, "google_sheets"), # Special type for Google Sheets
        "Analytic Report (.xlsx)": (analytic_report_new, False)
    }
    
    validation_warnings = []
    for label, (path_value, is_dir) in paths_to_validate.items():
        if path_value: # Only validate if a path is actually entered
            if is_dir == "google_sheets":
                from utils.google_sheets_utils import validate_google_sheets_url
                if not validate_google_sheets_url(path_value):
                    validation_warnings.append(f"URL для '{label}' не является корректной ссылкой Google Sheets: {path_value}")
            elif is_dir:
                if not os.path.isdir(path_value):
                    validation_warnings.append(f"Path for '{label}' is not a valid directory: {path_value}")
            else:
                if not os.path.isfile(path_value):
                    validation_warnings.append(f"File for '{label}' not found at: {path_value}")

    if validation_warnings:
        for warning in validation_warnings:
            st.warning(warning)
        st.info("Paths have been saved, but please double-check the warnings above. You can configure paths for files/directories you intend to create later.")

    # Update database path
    config_utils.set_db_path(db_path_new)
    
    # Update Ozon report paths
    config_utils.set_report_path("oz_barcodes_xlsx", oz_barcodes_new)
    config_utils.set_report_path("oz_orders_csv", oz_orders_new)
    config_utils.set_report_path("oz_prices_xlsx", oz_prices_new)
    config_utils.set_report_path("oz_products_csv", oz_products_new)
    
    # Update new Ozon folder paths
    config_utils.set_report_path("oz_category_products_folder", oz_category_products_new)
    config_utils.set_report_path("oz_video_products_folder", oz_video_products_new)
    config_utils.set_report_path("oz_video_cover_products_folder", oz_video_cover_products_new)
    
    # Update Wildberries report paths
    config_utils.set_report_path("wb_prices_xlsx", wb_prices_new)
    config_utils.set_report_path("wb_products_dir", wb_products_dir_new)
    
    # Update Google Sheets integration
    config_utils.set_report_path("punta_google_sheets_url", punta_sheets_url_new)
    
    # Update Analytic Report
    config_utils.set_report_path("analytic_report_xlsx", analytic_report_new)
    
    # Update Data Filters
    config_utils.set_data_filter("oz_category_products_brands", oz_brands_new)
    
    st.success("Settings saved successfully!")
    st.balloons() # A little celebration for saving
    # Optionally, re-run to reflect changes if not using session state for immediate updates across widgets
    # st.experimental_rerun()

# --- Database Cleanup Section --- 
st.markdown("---")
st.header("🗑️ Очистка базы данных")
st.info("Этот раздел позволяет очистить проблемные данные и оптимизировать размер базы данных. Все операции имеют множественные подтверждения для предотвращения случайного удаления.")

# Get database connection for cleanup operations
cleanup_db_connection = None
try:
    cleanup_db_connection = get_connection_and_ensure_schema()
except:
    pass

if not cleanup_db_connection:
    st.warning("⚠️ Нет соединения с базой данных. Настройте путь к БД выше для доступа к функциям очистки.")
else:
    # Show current database size
    import os
    from utils.config_utils import get_db_path
    
    db_path = get_db_path()
    if os.path.exists(db_path):
        current_db_size_mb = round(os.path.getsize(db_path) / (1024 * 1024), 2)
        st.info(f"📊 **Текущий размер базы данных: {current_db_size_mb} MB**")

    # Database analysis and recommendations
    with st.expander("📊 Анализ базы данных и рекомендации"):
        if st.button("🔍 Проанализировать БД", key="analyze_db_button"):
            with st.spinner("Анализ базы данных..."):
                from utils.db_cleanup import get_cleanup_recommendations
                
                analysis = get_cleanup_recommendations(cleanup_db_connection)
                
                if 'error' in analysis:
                    st.error(analysis['error'])
                else:
                    st.success(f"✅ Анализ завершен. Найдено {analysis['total_issues']} проблем.")
                    
                    # Display severity summary
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Всего проблем", analysis['total_issues'])
                    with col2:
                        st.metric("Критичные", analysis['severity_counts']['high'], delta_color="inverse")
                    with col3:
                        st.metric("Средние", analysis['severity_counts']['medium'], delta_color="inverse")
                    with col4:
                        st.metric("Низкие", analysis['severity_counts']['low'], delta_color="inverse")
                    
                    # Display recommendations
                    if analysis['recommendations']:
                        st.subheader("🎯 Рекомендации по очистке:")
                        
                        for rec in analysis['recommendations']:
                            severity_color = {
                                'high': '🔴',
                                'medium': '🟡', 
                                'low': '🟢'
                            }[rec['severity']]
                            
                            st.write(f"{severity_color} **{rec['description']}**")
                            if 'percentage' in rec:
                                st.progress(rec['percentage'] / 100)
                    else:
                        st.success("🎉 База данных не нуждается в очистке!")

    # Database compression and optimization
    st.subheader("🗜️ Сжатие и оптимизация базы данных")
    st.info("💡 После удаления данных необходимо сжать БД для освобождения места на диске")
    
    col_vacuum, col_optimize = st.columns(2)
    
    with col_vacuum:
        if st.button("🗜️ Сжать БД (VACUUM)", key="vacuum_db_button"):
            with st.spinner("Выполняется сжатие базы данных..."):
                from utils.db_cleanup import vacuum_database
                
                success, message, stats = vacuum_database(cleanup_db_connection)
                
                if success:
                    st.success(message)
                    st.json(stats)
                    # Update the displayed size
                    if os.path.exists(db_path):
                        new_size_mb = round(os.path.getsize(db_path) / (1024 * 1024), 2)
                        st.info(f"📊 **Новый размер базы данных: {new_size_mb} MB**")
                else:
                    st.error(message)
    
    with col_optimize:
        if st.button("⚡ Создать оптимизированную БД", key="optimize_db_button"):
            if 'confirm_optimize_db' not in st.session_state:
                st.session_state.confirm_optimize_db = False
            
            if not st.session_state.confirm_optimize_db:
                st.warning("⚠️ Будет создана новая оптимизированная БД (с резервной копией)")
                if st.button("✅ Подтвердить оптимизацию", key="confirm_optimize_db"):
                    st.session_state.confirm_optimize_db = True
                    st.rerun()
            else:
                with st.spinner("Создание оптимизированной базы данных..."):
                    from utils.db_cleanup import create_optimized_database
                    
                    success, message, stats = create_optimized_database(cleanup_db_connection)
                    
                    if success:
                        st.success(message)
                        st.json(stats)
                        # Update the displayed size
                        if os.path.exists(db_path):
                            new_size_mb = round(os.path.getsize(db_path) / (1024 * 1024), 2)
                            st.info(f"📊 **Новый размер базы данных: {new_size_mb} MB**")
                    else:
                        st.error(message)
                    
                    # Reset confirmation
                    st.session_state.confirm_optimize_db = False

    # Smart cleanup operations
    st.subheader("🤖 Умная очистка")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔧 Очистить дубли штрихкодов", key="cleanup_duplicate_barcodes_button"):
            if 'confirm_duplicate_barcodes' not in st.session_state:
                st.session_state.confirm_duplicate_barcodes = False
            
            if not st.session_state.confirm_duplicate_barcodes:
                st.warning("⚠️ Будут удалены дублирующиеся штрихкоды (оставлены последние записи)")
                if st.button("✅ Подтвердить очистку дублей", key="confirm_duplicate_barcodes"):
                    st.session_state.confirm_duplicate_barcodes = True
                    st.rerun()
            else:
                with st.spinner("Очистка дублей штрихкодов..."):
                    from utils.db_cleanup import cleanup_duplicate_barcodes
                    
                    success, message, stats = cleanup_duplicate_barcodes(cleanup_db_connection)
                    
                    if success:
                        st.success(message)
                        st.json(stats)
                    else:
                        st.error(message)
                    
                    # Reset confirmation
                    st.session_state.confirm_duplicate_barcodes = False

    with col2:
        if st.button("📅 Очистить заказы с будущими датами", key="cleanup_future_orders_button"):
            if 'confirm_future_orders' not in st.session_state:
                st.session_state.confirm_future_orders = False
            
            if not st.session_state.confirm_future_orders:
                st.warning("⚠️ Будут удалены заказы с датами после сегодняшнего дня")
                if st.button("✅ Подтвердить очистку заказов", key="confirm_future_orders"):
                    st.session_state.confirm_future_orders = True
                    st.rerun()
            else:
                with st.spinner("Очистка заказов с будущими датами..."):
                    from utils.db_cleanup import cleanup_future_dated_orders
                    
                    success, message, stats = cleanup_future_dated_orders(cleanup_db_connection)
                    
                    if success:
                        st.success(message)
                        st.json(stats)
                    else:
                        st.error(message)
                    
                    # Reset confirmation
                    st.session_state.confirm_future_orders = False

    col3, col4 = st.columns(2)
    
    with col3:
        if st.button("🧹 Очистить товары-сироты", key="cleanup_orphaned_products_button"):
            if 'confirm_orphaned_products' not in st.session_state:
                st.session_state.confirm_orphaned_products = False
            
            if not st.session_state.confirm_orphaned_products:
                st.warning("⚠️ Будут удалены товары без заказов и категорий")
                if st.button("✅ Подтвердить очистку товаров", key="confirm_orphaned_products"):
                    st.session_state.confirm_orphaned_products = True
                    st.rerun()
            else:
                with st.spinner("Очистка товаров-сирот..."):
                    from utils.db_cleanup import cleanup_orphaned_products
                    
                    success, message, stats = cleanup_orphaned_products(cleanup_db_connection)
                    
                    if success:
                        st.success(message)
                        st.json(stats)
                    else:
                        st.error(message)
                    
                    # Reset confirmation
                    st.session_state.confirm_orphaned_products = False

    with col4:
        if st.button("📝 Очистить малозаполненные поля", key="cleanup_empty_fields_button"):
            if 'confirm_empty_fields' not in st.session_state:
                st.session_state.confirm_empty_fields = False
            
            if not st.session_state.confirm_empty_fields:
                st.warning("⚠️ Будут очищены поля с заполненностью < 30%")
                field_to_clean = st.selectbox(
                    "Выберите поле для очистки:",
                    ["keywords", "rich_content_json"],
                    key="field_to_clean_select"
                )
                if st.button("✅ Подтвердить очистку полей", key="confirm_empty_fields"):
                    st.session_state.confirm_empty_fields = True
                    st.session_state.selected_field = field_to_clean
                    st.rerun()
            else:
                with st.spinner(f"Очистка поля {st.session_state.selected_field}..."):
                    from utils.db_cleanup import cleanup_empty_text_fields
                    
                    success, message, stats = cleanup_empty_text_fields(
                        cleanup_db_connection, 
                        "oz_category_products", 
                        st.session_state.selected_field
                    )
                    
                    if success:
                        st.success(message)
                        st.json(stats)
                    else:
                        st.error(message)
                    
                    # Reset confirmation
                    st.session_state.confirm_empty_fields = False

    # Dangerous operations - complete table clearing
    st.subheader("⚠️ Полная очистка таблиц")
    st.warning("ВНИМАНИЕ: Операция полностью удаляет все данные из выбранной таблицы!")
    
    # Table selection for complete clearing
    table_options = [
        "oz_orders", "oz_barcodes", "oz_products", "oz_category_products",
        "oz_video_products", "oz_video_cover_products", "wb_products", 
        "wb_prices", "punta_table"
    ]
    
    col_table, col_confirm = st.columns([1, 1])
    
    with col_table:
        selected_table_to_clear = st.selectbox(
            "Выберите таблицу:",
            ["Выберите таблицу..."] + table_options,
            key="table_to_clear_select"
        )
    
    with col_confirm:
        if selected_table_to_clear != "Выберите таблицу...":
            table_name_input = st.text_input(
                f"Введите '{selected_table_to_clear}' для подтверждения:",
                key="table_name_confirm",
                placeholder=selected_table_to_clear
            )
    
    if selected_table_to_clear != "Выберите таблицу..." and table_name_input == selected_table_to_clear:
        if st.button(f"🗑️ Очистить таблицу {selected_table_to_clear}", key="execute_table_clear", type="primary"):
            with st.spinner(f"Очистка таблицы {selected_table_to_clear}..."):
                from utils.db_cleanup import clear_table_completely
                
                success, message, stats = clear_table_completely(
                    cleanup_db_connection, 
                    selected_table_to_clear
                )
                
                if success:
                    st.success(message)
                    st.json(stats)
                else:
                    st.error(message)
    elif selected_table_to_clear != "Выберите таблицу..." and table_name_input and table_name_input != selected_table_to_clear:
        st.error("❌ Название таблицы не совпадает!")

# Display current config for verification (optional)
# with st.expander("Current Configuration (from config.json)"):
#     st.json(config_utils.load_config()) 