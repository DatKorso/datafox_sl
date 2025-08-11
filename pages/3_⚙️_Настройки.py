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

# --- Margin Calculation Parameters ---
with st.expander("Margin Calculation Parameters"):
    st.subheader("Настройки расчета маржинальности")
    st.info("Настройте параметры для расчета маржинальности товаров в менеджере рекламы Ozon.")
    
    # Load current margin configuration
    margin_config = config_utils.get_margin_config()
    
    col1, col2 = st.columns(2)
    
    with col1:
        commission_current = margin_config.get("commission_percent", 36.0)
        commission_new = st.number_input(
            "Комиссия (%)", 
            value=commission_current, 
            min_value=0.0, 
            max_value=100.0, 
            step=0.1,
            help="Процент комиссии маркетплейса Ozon. По умолчанию: 36%"
        )
        
        acquiring_current = margin_config.get("acquiring_percent", 0.0)
        acquiring_new = st.number_input(
            "Эквайринг (%)", 
            value=acquiring_current, 
            min_value=0.0, 
            max_value=100.0, 
            step=0.1,
            help="Процент эквайринга (банковские комиссии). По умолчанию: 0%"
        )
        
        advertising_current = margin_config.get("advertising_percent", 3.0)
        advertising_new = st.number_input(
            "Реклама (%)", 
            value=advertising_current, 
            min_value=0.0, 
            max_value=100.0, 
            step=0.1,
            help="Процент затрат на рекламу. По умолчанию: 3%"
        )
    
    with col2:
        vat_current = margin_config.get("vat_percent", 20.0)
        vat_new = st.number_input(
            "НДС (%)", 
            value=vat_current, 
            min_value=0.0, 
            max_value=100.0, 
            step=0.1,
            help="Процент налога на добавленную стоимость. По умолчанию: 20%"
        )
        
        exchange_rate_current = margin_config.get("exchange_rate", 90.0)
        exchange_rate_new = st.number_input(
            "Курс валюты (руб/USD)", 
            value=exchange_rate_current, 
            min_value=1.0, 
            max_value=1000.0, 
            step=0.1,
            help="Курс доллара к рублю для конвертации себестоимости. По умолчанию: 90"
        )
    
    # Display current formula for reference
    st.markdown("**Формула расчета маржинальности:**")
    st.code("""
margin = (((oz_price/(1+VAT/100) - (oz_price*((Commission+Acquiring+Advertising)/100))/1.2)/ExchangeRate) - cost_price_usd) / cost_price_usd * 100
    """, language="text")
    
    # Test calculation button
    if st.button("🧮 Тестовый расчет", key="test_margin_calculation"):
        test_oz_price = 1000.0  # Test price in rubles
        test_cost_usd = 5.0     # Test cost in USD
        
        try:
            # Calculate using current form values
            vat_decimal = vat_new / 100
            commission_sum = (commission_new + acquiring_new + advertising_new) / 100
            
            # Apply the formula
            price_after_vat = test_oz_price / (1 + vat_decimal)
            commission_amount = test_oz_price * commission_sum / 1.2
            net_price_rub = price_after_vat - commission_amount
            net_price_usd = net_price_rub / exchange_rate_new
            margin_decimal = (net_price_usd - test_cost_usd) / test_cost_usd
            margin_percent = margin_decimal * 100
            
            st.success(f"✅ Тестовый расчет: при цене {test_oz_price} руб и себестоимости ${test_cost_usd} маржинальность составит {margin_percent:.1f}%")
            
            # Show calculation breakdown
            with st.expander("Детали расчета"):
                st.write(f"• Цена товара: {test_oz_price} руб")
                st.write(f"• Цена без НДС: {price_after_vat:.2f} руб")
                st.write(f"• Комиссии: {commission_amount:.2f} руб")
                st.write(f"• Чистая выручка: {net_price_rub:.2f} руб")
                st.write(f"• Чистая выручка в USD: ${net_price_usd:.2f}")
                st.write(f"• Себестоимость: ${test_cost_usd}")
                st.write(f"• Маржинальность: {margin_percent:.1f}%")
                
        except Exception as e:
            st.error(f"❌ Ошибка в тестовом расчете: {e}")
    
    # Add validation button for punta_table availability
    if st.button("🔍 Проверить доступность данных Punta", key="validate_punta_table"):
        try:
            conn = get_connection()
            if not conn:
                st.error("❌ Нет подключения к базе данных.")
            else:
                # Check if punta_table exists
                try:
                    table_exists_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='punta_table'"
                    table_check = conn.execute(table_exists_query).fetchdf()
                    
                    if table_check.empty:
                        st.warning("⚠️ Таблица punta_table не найдена в базе данных. Расчет маржинальности будет недоступен.")
                        st.info("💡 Используйте функцию импорта Google Sheets ниже для загрузки данных Punta.")
                    else:
                        # Check table structure
                        columns_query = "PRAGMA table_info(punta_table)"
                        columns_info = conn.execute(columns_query).fetchdf()
                        available_columns = columns_info['name'].tolist()
                        
                        required_columns = ['wb_sku', 'cost_price_usd']
                        missing_columns = [col for col in required_columns if col not in available_columns]
                        
                        if missing_columns:
                            st.error(f"❌ В таблице punta_table отсутствуют необходимые колонки: {', '.join(missing_columns)}")
                        else:
                            # Check data availability
                            count_query = "SELECT COUNT(*) as total_rows FROM punta_table WHERE cost_price_usd IS NOT NULL AND TRIM(cost_price_usd) != ''"
                            count_result = conn.execute(count_query).fetchdf()
                            total_rows = count_result['total_rows'].iloc[0] if not count_result.empty else 0
                            
                            if total_rows == 0:
                                st.warning("⚠️ Таблица punta_table существует, но не содержит данных о себестоимости.")
                            else:
                                st.success(f"✅ Таблица punta_table доступна с {total_rows} записями о себестоимости.")
                                
                                # Show sample data
                                sample_query = "SELECT wb_sku, cost_price_usd FROM punta_table WHERE cost_price_usd IS NOT NULL AND TRIM(cost_price_usd) != '' LIMIT 5"
                                sample_data = conn.execute(sample_query).fetchdf()
                                
                                if not sample_data.empty:
                                    st.write("**Образец данных:**")
                                    st.dataframe(sample_data, use_container_width=True)
                        
                except Exception as table_error:
                    st.error(f"❌ Ошибка при проверке таблицы punta_table: {table_error}")
                    
        except Exception as e:
            st.error(f"❌ Ошибка при проверке доступности данных Punta: {e}")

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
    st.subheader("Cards & Ratings")
    oz_card_rating_current = config_utils.get_report_path("oz_card_rating_xlsx")
    oz_card_rating_new = st.text_input("Ozon Card Rating (.xlsx)", value=oz_card_rating_current, placeholder="Path to ozon_card_rating.xlsx", help="Full path to the Ozon card rating Excel file. The file should contain columns: RezonitemID, Артикул, Рейтинг (1), Кол-во отзывов.")

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
        "Ozon Card Rating (.xlsx)": (oz_card_rating_new, False),
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
    
    # Update margin calculation parameters with validation
    try:
        margin_config_new = {
            "commission_percent": commission_new,
            "acquiring_percent": acquiring_new,
            "advertising_percent": advertising_new,
            "vat_percent": vat_new,
            "exchange_rate": exchange_rate_new
        }
        
        # Validate margin configuration before saving
        validation_errors = []
        
        if not (0 <= commission_new <= 100):
            validation_errors.append(f"Комиссия должна быть от 0% до 100%, получено: {commission_new}%")
        
        if not (0 <= acquiring_new <= 100):
            validation_errors.append(f"Эквайринг должен быть от 0% до 100%, получено: {acquiring_new}%")
        
        if not (0 <= advertising_new <= 100):
            validation_errors.append(f"Реклама должна быть от 0% до 100%, получено: {advertising_new}%")
        
        if not (0 <= vat_new <= 100):
            validation_errors.append(f"НДС должен быть от 0% до 100%, получено: {vat_new}%")
        
        if not (1 <= exchange_rate_new <= 1000):
            validation_errors.append(f"Курс валюты должен быть от 1 до 1000, получено: {exchange_rate_new}")
        
        # Check if total fees are reasonable
        total_fees = commission_new + acquiring_new + advertising_new
        if total_fees > 80:
            validation_errors.append(f"Общая сумма комиссий ({total_fees:.1f}%) кажется слишком высокой")
        
        if validation_errors:
            for error in validation_errors:
                st.error(f"❌ {error}")
            st.warning("⚠️ Параметры маржинальности не сохранены из-за ошибок валидации.")
        else:
            config_utils.set_margin_config(margin_config_new)
            st.success("✅ Параметры маржинальности сохранены успешно.")
            
    except Exception as e:
        st.error(f"❌ Ошибка при сохранении параметров маржинальности: {e}")
        print(f"DEBUG: Error saving margin config: {e}")
    
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
    
    # Update Cards & Ratings
    config_utils.set_report_path("oz_card_rating_xlsx", oz_card_rating_new)
    
    # Update Analytic Report
    config_utils.set_report_path("analytic_report_xlsx", analytic_report_new)
    
    # Update Data Filters
    config_utils.set_data_filter("oz_category_products_brands", oz_brands_new)
    
    st.success("Settings saved successfully!")
    st.balloons() # A little celebration for saving
    # Optionally, re-run to reflect changes if not using session state for immediate updates across widgets
    # st.experimental_rerun()
