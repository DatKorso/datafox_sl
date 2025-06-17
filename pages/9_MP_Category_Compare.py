"""
Streamlit page for comparing product categories between marketplaces.

This page allows users to:
- Input Wildberries or Ozon SKUs to find linked products
- Manage category mapping table between WB and Ozon
- View category discrepancies between linked products
- Edit category mappings directly in the interface
"""
import streamlit as st
import pandas as pd
from utils.db_connection import connect_db
from utils.db_search_helpers import get_normalized_wb_barcodes, get_ozon_barcodes_and_identifiers
from utils.category_helpers import (
    get_unique_wb_categories, get_unique_oz_categories, suggest_category_mappings,
    get_unmapped_categories, get_category_usage_stats, validate_category_mapping,
    export_category_mappings_to_csv, import_category_mappings_from_csv
)
from datetime import datetime

st.set_page_config(page_title="Category Compare - Marketplace Analyzer", layout="wide")
st.title("🔄 Сверка категорий между маркетплейсами")
st.markdown("---")

# --- Database Connection ---
def get_db_connection():
    """Get database connection for category comparison operations."""
    conn = connect_db()
    if not conn:
        st.error("База данных не подключена. Пожалуйста, настройте базу данных в Settings.")
        if st.button("Перейти в Settings", key="db_settings_button_cat"):
            st.switch_page("pages/3_Settings.py")
        st.stop()
    return conn

# Initialize connection - will be used throughout the page
conn = get_db_connection()

# --- Helper Functions ---

def create_category_mapping_table_if_not_exists(db_conn):
    """Creates category_mapping table if it doesn't exist."""
    try:
        # Verify connection
        if not db_conn:
            st.error("Соединение с базой данных недоступно")
            return False
            
        # Check if table exists
        result = db_conn.execute("SELECT table_name FROM information_schema.tables WHERE table_name = 'category_mapping';").fetchone()
        
        if not result:
            # Create sequence first
            db_conn.execute("CREATE SEQUENCE IF NOT EXISTS category_mapping_seq START 1")
            
            # Create table
            create_sql = """
            CREATE TABLE category_mapping (
                id INTEGER PRIMARY KEY DEFAULT nextval('category_mapping_seq'),
                wb_category VARCHAR,
                oz_category VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                notes TEXT
            );
            """
            db_conn.execute(create_sql)
            st.success("Таблица соответствий категорий создана успешно")
        return True
    except Exception as e:
        st.error(f"Ошибка при создании таблицы: {e}")
        return False

def get_category_mappings(db_conn):
    """Gets all category mappings from database."""
    try:
        query = """
        SELECT id, wb_category, oz_category, created_at, notes 
        FROM category_mapping 
        ORDER BY wb_category, oz_category
        """
        return db_conn.execute(query).fetchdf()
    except Exception as e:
        st.error(f"Ошибка при получении соответствий категорий: {e}")
        return pd.DataFrame()

def add_category_mapping(db_conn, wb_category, oz_category, notes=""):
    """Adds new category mapping."""
    try:
        # Check if mapping already exists
        check_query = "SELECT id FROM category_mapping WHERE wb_category = ? AND oz_category = ?"
        existing = db_conn.execute(check_query, [wb_category, oz_category]).fetchone()
        
        if existing:
            st.warning(f"Соответствие '{wb_category}' ↔ '{oz_category}' уже существует")
            return False
        
        insert_query = """
        INSERT INTO category_mapping (wb_category, oz_category, notes) 
        VALUES (?, ?, ?)
        """
        db_conn.execute(insert_query, [wb_category, oz_category, notes])
        st.success(f"Добавлено соответствие: '{wb_category}' ↔ '{oz_category}'")
        return True
    except Exception as e:
        st.error(f"Ошибка при добавлении соответствия: {e}")
        return False

def delete_category_mapping(db_conn, mapping_id):
    """Deletes category mapping by ID."""
    try:
        delete_query = "DELETE FROM category_mapping WHERE id = ?"
        db_conn.execute(delete_query, [mapping_id])
        st.success("Соответствие удалено")
        return True
    except Exception as e:
        st.error(f"Ошибка при удалении соответствия: {e}")
        return False

def find_linked_products_with_categories(db_conn, input_skus, search_by="wb_sku"):
    """
    Finds linked products between marketplaces and gets their categories.
    
    Args:
        db_conn: Database connection
        input_skus: List of SKUs to search for
        search_by: "wb_sku" or "oz_sku"
        
    Returns:
        DataFrame with linked products and their categories
    """
    if not input_skus:
        return pd.DataFrame()
    
    try:
        # Verify connection is still active
        if not db_conn:
            st.error("Соединение с базой данных недоступно")
            return pd.DataFrame()
        
        # Test connection with a simple query
        try:
            db_conn.execute("SELECT 1").fetchone()
        except Exception as e:
            st.error(f"Соединение с базой данных потеряно: {e}")
            return pd.DataFrame()
        
        if search_by == "wb_sku":
            # Find linked Ozon products via barcodes
            try:
                wb_barcodes_df = get_normalized_wb_barcodes(db_conn, wb_skus=input_skus)
            except Exception as e:
                st.error(f"Ошибка при получении штрихкодов WB: {e}")
                return pd.DataFrame()
                
            if wb_barcodes_df.empty:
                st.info("Не найдены штрихкоды для указанных артикулов WB")
                return pd.DataFrame()
            
            try:
                oz_barcodes_ids_df = get_ozon_barcodes_and_identifiers(db_conn)
            except Exception as e:
                st.error(f"Ошибка при получении штрихкодов Ozon: {e}")
                return pd.DataFrame()
                
            if oz_barcodes_ids_df.empty:
                st.info("В базе данных отсутствуют штрихкоды Ozon")
                return pd.DataFrame()
            
            # Prepare data for merge
            wb_barcodes_df = wb_barcodes_df.rename(columns={'individual_barcode_wb': 'barcode'})
            oz_barcodes_ids_df = oz_barcodes_ids_df.rename(columns={'oz_barcode': 'barcode'})
            
            # Ensure barcode consistency
            wb_barcodes_df['barcode'] = wb_barcodes_df['barcode'].astype(str).str.strip()
            oz_barcodes_ids_df['barcode'] = oz_barcodes_ids_df['barcode'].astype(str).str.strip()
            wb_barcodes_df['wb_sku'] = wb_barcodes_df['wb_sku'].astype(str)
            oz_barcodes_ids_df['oz_sku'] = oz_barcodes_ids_df['oz_sku'].astype(str)
            
            # Remove empty barcodes and duplicates
            wb_barcodes_df = wb_barcodes_df[wb_barcodes_df['barcode'] != ''].drop_duplicates()
            oz_barcodes_ids_df = oz_barcodes_ids_df[oz_barcodes_ids_df['barcode'] != ''].drop_duplicates()
            
            # Join on common barcodes
            linked_df = pd.merge(wb_barcodes_df, oz_barcodes_ids_df, on='barcode', how='inner')
            
            if linked_df.empty:
                return pd.DataFrame()
            
            # Get WB categories
            wb_skus_list = linked_df['wb_sku'].unique().tolist()
            wb_categories_query = f"""
            SELECT wb_sku, wb_category 
            FROM wb_products 
            WHERE wb_sku IN ({', '.join(['?'] * len(wb_skus_list))})
            """
            wb_categories_df = db_conn.execute(wb_categories_query, wb_skus_list).fetchdf()
            wb_categories_df['wb_sku'] = wb_categories_df['wb_sku'].astype(str)
            
            # Get Ozon categories (from oz_category_products table via oz_vendor_code)
            # and link oz_vendor_code to oz_sku using oz_products table
            oz_vendor_codes_list = linked_df['oz_vendor_code'].unique().tolist()
            oz_categories_query = f"""
            SELECT 
                cp.oz_vendor_code, 
                cp.type as oz_category,
                p.oz_sku
            FROM oz_category_products cp
            LEFT JOIN oz_products p ON cp.oz_vendor_code = p.oz_vendor_code
            WHERE cp.oz_vendor_code IN ({', '.join(['?'] * len(oz_vendor_codes_list))})
            """
            oz_categories_df = db_conn.execute(oz_categories_query, oz_vendor_codes_list).fetchdf()
            if 'oz_sku' in oz_categories_df.columns:
                oz_categories_df['oz_sku'] = oz_categories_df['oz_sku'].astype(str)
            
            # Merge all data
            result_df = linked_df[['wb_sku', 'oz_sku', 'oz_vendor_code', 'barcode']].drop_duplicates()
            result_df = pd.merge(result_df, wb_categories_df, on='wb_sku', how='left')
            result_df = pd.merge(result_df, oz_categories_df, on='oz_vendor_code', how='left')
            
            # Update oz_sku from the category join if it's more accurate
            result_df['oz_sku'] = result_df['oz_sku_y'].fillna(result_df['oz_sku_x'])
            result_df = result_df.drop(columns=['oz_sku_x', 'oz_sku_y'], errors='ignore')
            
        else:  # search_by == "oz_sku"
            # Similar logic but starting from Ozon SKUs
            oz_skus_for_query = list(set(input_skus))
            
            # Get Ozon products with categories and barcodes
            # Use oz_products as the main table and join categories via oz_vendor_code
            oz_query = f"""
            SELECT DISTINCT
                p.oz_sku,
                p.oz_vendor_code,
                cp.type as oz_category,
                b.oz_barcode as barcode
            FROM oz_products p
            LEFT JOIN oz_category_products cp ON p.oz_vendor_code = cp.oz_vendor_code
            LEFT JOIN oz_barcodes b ON p.oz_product_id = b.oz_product_id
            WHERE p.oz_sku IN ({', '.join(['?'] * len(oz_skus_for_query))})
                AND NULLIF(TRIM(b.oz_barcode), '') IS NOT NULL
            """
            
            oz_data_df = db_conn.execute(oz_query, oz_skus_for_query).fetchdf()
            
            if oz_data_df.empty:
                return pd.DataFrame()
            
            # Get WB products with same barcodes
            wb_query = f"""
            SELECT 
                p.wb_sku,
                p.wb_category,
                TRIM(b.barcode) AS barcode
            FROM wb_products p,
            UNNEST(regexp_split_to_array(COALESCE(p.wb_barcodes, ''), E'[\\s;]+')) AS b(barcode)
            WHERE NULLIF(TRIM(b.barcode), '') IS NOT NULL
            """
            
            wb_data_df = db_conn.execute(wb_query).fetchdf()
            
            if wb_data_df.empty:
                return pd.DataFrame()
            
            # Ensure data types
            oz_data_df['barcode'] = oz_data_df['barcode'].astype(str).str.strip()
            wb_data_df['barcode'] = wb_data_df['barcode'].astype(str).str.strip()
            oz_data_df['oz_sku'] = oz_data_df['oz_sku'].astype(str)
            wb_data_df['wb_sku'] = wb_data_df['wb_sku'].astype(str)
            
            # Remove empty barcodes
            oz_data_df = oz_data_df[oz_data_df['barcode'] != ''].drop_duplicates()
            wb_data_df = wb_data_df[wb_data_df['barcode'] != ''].drop_duplicates()
            
            # Join on common barcodes
            result_df = pd.merge(oz_data_df, wb_data_df, on='barcode', how='inner')
        
        return result_df
        
    except Exception as e:
        st.error(f"Ошибка при поиске связанных товаров: {e}")
        return pd.DataFrame()

def analyze_category_discrepancies(db_conn, linked_products_df, show_all=False):
    """
    Analyzes category discrepancies based on category mapping table.
    
    Args:
        db_conn: Database connection
        linked_products_df: DataFrame with linked products
        show_all: If True, shows all matches including correct ones
    
    Returns:
        DataFrame with discrepancies (or all matches if show_all=True)
    """
    if linked_products_df.empty:
        return pd.DataFrame()
    
    try:
        # Get category mappings
        mappings_df = get_category_mappings(db_conn)
        
        if mappings_df.empty:
            st.warning("Таблица соответствий категорий пуста. Добавьте соответствия для анализа.")
            return pd.DataFrame()
        
        # Create mapping dictionary
        category_map = dict(zip(mappings_df['wb_category'], mappings_df['oz_category']))
        
        # Analyze all products
        results = []
        
        for _, row in linked_products_df.iterrows():
            wb_category = row.get('wb_category', '')
            oz_category = row.get('oz_category', '')
            
            if pd.isna(wb_category) or pd.isna(oz_category):
                if show_all:
                    results.append({
                        'wb_sku': row['wb_sku'],
                        'oz_sku': row['oz_sku'],
                        'oz_vendor_code': row.get('oz_vendor_code', ''),
                        'barcode': row['barcode'],
                        'wb_category': str(wb_category) if not pd.isna(wb_category) else 'Нет данных',
                        'oz_category_actual': str(oz_category) if not pd.isna(oz_category) else 'Нет данных',
                        'oz_category_expected': 'Нет данных',
                        'discrepancy_type': 'Отсутствуют данные о категориях',
                        'status': '❌ Ошибка'
                    })
                continue
                
            wb_category = str(wb_category).strip()
            oz_category = str(oz_category).strip()
            
            # Check if WB category has a mapping
            expected_oz_category = category_map.get(wb_category)
            
            if expected_oz_category:
                if expected_oz_category == oz_category:
                    # Categories match correctly
                    if show_all:
                        results.append({
                            'wb_sku': row['wb_sku'],
                            'oz_sku': row['oz_sku'],
                            'oz_vendor_code': row.get('oz_vendor_code', ''),
                            'barcode': row['barcode'],
                            'wb_category': wb_category,
                            'oz_category_actual': oz_category,
                            'oz_category_expected': expected_oz_category,
                            'discrepancy_type': 'Категории соответствуют',
                            'status': '✅ Корректно'
                        })
                else:
                    # Categories don't match
                    results.append({
                        'wb_sku': row['wb_sku'],
                        'oz_sku': row['oz_sku'],
                        'oz_vendor_code': row.get('oz_vendor_code', ''),
                        'barcode': row['barcode'],
                        'wb_category': wb_category,
                        'oz_category_actual': oz_category,
                        'oz_category_expected': expected_oz_category,
                        'discrepancy_type': 'Несоответствие категорий',
                        'status': '⚠️ Расхождение'
                    })
            else:
                # No mapping configured for this WB category
                results.append({
                    'wb_sku': row['wb_sku'],
                    'oz_sku': row['oz_sku'],
                    'oz_vendor_code': row.get('oz_vendor_code', ''),
                    'barcode': row['barcode'],
                    'wb_category': wb_category,
                    'oz_category_actual': oz_category,
                    'oz_category_expected': 'Не настроено',
                    'discrepancy_type': 'Отсутствует соответствие',
                    'status': '❓ Не настроено'
                })
        
        return pd.DataFrame(results)
        
    except Exception as e:
        st.error(f"Ошибка при анализе категорий: {e}")
        return pd.DataFrame()

# Initialize database table
create_category_mapping_table_if_not_exists(conn)

# --- Main UI ---

# Tabs for different functionality
tab1, tab2, tab3 = st.tabs(["🔍 Анализ категорий", "⚙️ Управление соответствиями", "📊 Статистика"])

with tab1:
    st.header("Анализ соответствия категорий")
    
    # Input section
    col1, col2 = st.columns([2, 1])
    
    with col1:
        search_method = st.radio(
            "Поиск по:",
            ["Артикулам WB (wb_sku)", "Артикулам Ozon (oz_sku)"],
            index=0
        )
        
        search_by = "wb_sku" if "WB" in search_method else "oz_sku"
        
        input_skus_text = st.text_area(
            f"Введите {search_method.lower()} (один на строку или через пробел):",
            height=100,
            help="Например:\n123456\n789012\nили\n123456 789012"
        )
    
    with col2:
        st.info("""
        **Как это работает:**
        1. Введите артикулы
        2. Система найдет связанные товары через штрихкоды
        3. Сравнит категории с таблицей соответствий
        4. Покажет расхождения
        """)
    
    # Options for analysis
    show_all_matches = st.checkbox(
        "🔍 Показать все найденные товары (включая правильные соответствия)",
        value=False,
        help="По умолчанию показываются только товары с расхождениями. "
             "Включите эту опцию, чтобы видеть все найденные связанные товары и их статус."
    )
    
    button_text = "🚀 Найти все соответствия категорий" if show_all_matches else "🚀 Найти расхождения в категориях"
    
    if st.button(button_text, type="primary"):
        if not input_skus_text.strip():
            st.warning("Пожалуйста, введите артикулы для анализа")
        else:
            # Parse input SKUs
            input_skus = []
            for line in input_skus_text.strip().split('\n'):
                for sku in line.split():
                    sku = sku.strip()
                    if sku:
                        input_skus.append(sku)
            
            if not input_skus:
                st.warning("Не найдено валидных артикулов")
            else:
                with st.spinner("Поиск связанных товаров и анализ категорий..."):
                    # Create fresh connection for this operation
                    analysis_conn = get_db_connection()
                    
                    try:
                        # Find linked products
                        linked_df = find_linked_products_with_categories(analysis_conn, input_skus, search_by)
                        
                        if linked_df.empty:
                            st.info("Не найдено связанных товаров для анализа")
                        else:
                            st.success(f"Найдено {len(linked_df)} связанных товаров")
                            
                            # Analyze category matches/discrepancies
                            results_df = analyze_category_discrepancies(analysis_conn, linked_df, show_all=show_all_matches)
                            
                            if results_df.empty:
                                if show_all_matches:
                                    st.info("Не найдено товаров для анализа категорий")
                                else:
                                    st.success("🎉 Расхождений в категориях не найдено!")
                            else:
                                # Filter for statistics
                                if show_all_matches:
                                    correct_matches = results_df[results_df['status'] == '✅ Корректно']
                                    discrepancies = results_df[results_df['status'].isin(['⚠️ Расхождение', '❓ Не настроено', '❌ Ошибка'])]
                                    
                                    # Show summary
                                    col1, col2, col3 = st.columns(3)
                                    with col1:
                                        st.metric("Всего товаров", len(results_df))
                                    with col2:
                                        st.metric("Корректных соответствий", len(correct_matches), 
                                                 delta=f"{len(correct_matches)/len(results_df)*100:.1f}%")
                                    with col3:
                                        st.metric("Проблемных", len(discrepancies), 
                                                 delta=f"{len(discrepancies)/len(results_df)*100:.1f}%")
                                    
                                    st.subheader("Все найденные соответствия категорий")
                                else:
                                    st.warning(f"⚠️ Найдено {len(results_df)} расхождений в категориях")
                                    st.subheader("Расхождения в категориях")
                                
                                # Display results
                                display_columns = [
                                    'status', 'wb_sku', 'oz_sku', 'oz_vendor_code', 
                                    'wb_category', 'oz_category_actual', 'oz_category_expected',
                                    'discrepancy_type'
                                ]
                                
                                # Color-code the status column for better visibility
                                st.dataframe(
                                    results_df[display_columns],
                                    use_container_width=True,
                                    hide_index=True,
                                    column_config={
                                        'status': st.column_config.TextColumn('Статус', width="small"),
                                        'wb_sku': 'WB SKU',
                                        'oz_sku': 'Ozon SKU', 
                                        'oz_vendor_code': 'Код поставщика',
                                        'wb_category': 'Категория WB',
                                        'oz_category_actual': 'Категория Ozon (факт)',
                                        'oz_category_expected': 'Категория Ozon (ожидается)',
                                        'discrepancy_type': 'Описание'
                                    }
                                )
                                
                                # Filter options for all matches view
                                if show_all_matches and len(results_df) > 10:
                                    st.subheader("Фильтры")
                                    
                                    filter_col1, filter_col2 = st.columns(2)
                                    
                                    with filter_col1:
                                        status_filter = st.multiselect(
                                            "Фильтр по статусу:",
                                            options=results_df['status'].unique(),
                                            default=results_df['status'].unique(),
                                            key="status_filter"
                                        )
                                    
                                    with filter_col2:
                                        wb_category_filter = st.selectbox(
                                            "Фильтр по категории WB:",
                                            options=["Все"] + list(results_df['wb_category'].unique()),
                                            index=0,
                                            key="wb_category_filter"
                                        )
                                    
                                    # Apply filters
                                    filtered_df = results_df[results_df['status'].isin(status_filter)]
                                    if wb_category_filter != "Все":
                                        filtered_df = filtered_df[filtered_df['wb_category'] == wb_category_filter]
                                    
                                    if len(filtered_df) != len(results_df):
                                        st.subheader("Отфильтрованные результаты")
                                        st.dataframe(
                                            filtered_df[display_columns],
                                            use_container_width=True,
                                            hide_index=True,
                                            column_config={
                                                'status': st.column_config.TextColumn('Статус', width="small"),
                                                'wb_sku': 'WB SKU',
                                                'oz_sku': 'Ozon SKU', 
                                                'oz_vendor_code': 'Код поставщика',
                                                'wb_category': 'Категория WB',
                                                'oz_category_actual': 'Категория Ozon (факт)',
                                                'oz_category_expected': 'Категория Ozon (ожидается)',
                                                'discrepancy_type': 'Описание'
                                            }
                                        )
                                
                                # Export option
                                export_label = "📥 Экспортировать все результаты в Excel" if show_all_matches else "📥 Экспортировать расхождения в Excel"
                                
                                if st.button(export_label):
                                    try:
                                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                        prefix = "category_analysis" if show_all_matches else "category_discrepancies"
                                        filename = f"{prefix}_{timestamp}.xlsx"
                                        results_df.to_excel(filename, index=False)
                                        
                                        with open(filename, "rb") as file:
                                            st.download_button(
                                                label="Скачать файл Excel",
                                                data=file.read(),
                                                file_name=filename,
                                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                            )
                                    except Exception as e:
                                        st.error(f"Ошибка при экспорте: {e}")
                    
                    except Exception as e:
                        st.error(f"Ошибка при анализе категорий: {e}")
                        st.error("Попробуйте обновить страницу или обратитесь к администратору")
                    
                    finally:
                        # Close the analysis connection
                        if analysis_conn:
                            analysis_conn.close()

with tab2:
    st.header("Управление соответствиями категорий")
    
    # Quick statistics
    wb_categories = get_unique_wb_categories(conn)
    oz_categories = get_unique_oz_categories(conn)
    mappings_df = get_category_mappings(conn)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Категорий WB", len(wb_categories))
    with col2:
        st.metric("Категорий Ozon", len(oz_categories))
    with col3:
        st.metric("Соответствий", len(mappings_df))
    
    # Tabs for different management options
    mgmt_tab1, mgmt_tab2, mgmt_tab3, mgmt_tab4 = st.tabs([
        "➕ Добавить", "🔍 Автопредложения", "📋 Просмотр", "📤 Импорт/Экспорт"
    ])
    
    with mgmt_tab1:
        st.subheader("Добавить новое соответствие")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Выберите категорию WB:**")
            if wb_categories:
                new_wb_category = st.selectbox(
                    "Категории из базы данных:",
                    [""] + wb_categories,
                    key="select_wb_cat"
                )
            else:
                st.info("Категории WB не найдены в базе данных")
                new_wb_category = ""
            
            # Alternative text input
            custom_wb_category = st.text_input(
                "Или введите вручную:",
                key="custom_wb_cat",
                help="Если нужной категории нет в списке"
            )
            
            final_wb_category = custom_wb_category if custom_wb_category.strip() else new_wb_category
        
        with col2:
            st.write("**Выберите категорию Ozon:**")
            if oz_categories:
                new_oz_category = st.selectbox(
                    "Категории из базы данных:",
                    [""] + oz_categories,
                    key="select_oz_cat"
                )
            else:
                st.info("Категории Ozon не найдены в базе данных")
                new_oz_category = ""
            
            # Alternative text input
            custom_oz_category = st.text_input(
                "Или введите вручную:",
                key="custom_oz_cat",
                help="Если нужной категории нет в списке"
            )
            
            final_oz_category = custom_oz_category if custom_oz_category.strip() else new_oz_category
        
        # Notes field
        mapping_notes = st.text_area(
            "Комментарии (опционально):",
            key="mapping_notes",
            help="Дополнительная информация о соответствии"
        )
        
        # Validation
        if final_wb_category and final_oz_category:
            validation = validate_category_mapping(conn, final_wb_category, final_oz_category)
            
            if validation['warnings']:
                for warning in validation['warnings']:
                    st.warning(warning)
            
            if validation['wb_exists'] and validation['oz_exists']:
                st.success(f"✅ Обе категории найдены в БД (WB: {validation['wb_product_count']} товаров, Ozon: {validation['oz_product_count']} товаров)")
        
        # Add button
        if st.button("Добавить соответствие", type="primary", key="add_mapping"):
            if final_wb_category.strip() and final_oz_category.strip():
                if add_category_mapping(conn, final_wb_category.strip(), final_oz_category.strip(), mapping_notes.strip()):
                    st.rerun()
            else:
                st.warning("Заполните обе категории")
    
    with mgmt_tab2:
        st.subheader("Автоматические предложения")
        st.info("Система анализирует названия категорий и предлагает возможные соответствия на основе сходства текста")
        
        similarity_threshold = st.slider(
            "Минимальное сходство:",
            min_value=0.5,
            max_value=1.0,
            value=0.7,
            step=0.05,
            help="Чем выше значение, тем более похожими должны быть названия категорий"
        )
        
        if st.button("🔍 Найти предложения", key="find_suggestions"):
            with st.spinner("Анализ категорий и поиск соответствий..."):
                suggestions = suggest_category_mappings(conn, similarity_threshold)
                
                if suggestions:
                    st.success(f"Найдено {len(suggestions)} предложений")
                    
                    suggestions_df = pd.DataFrame(suggestions)
                    
                    st.dataframe(
                        suggestions_df,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            'wb_category': 'Категория WB',
                            'oz_category': 'Категория Ozon',
                            'similarity': st.column_config.NumberColumn(
                                'Сходство',
                                format="%.2f"
                            ),
                            'confidence': 'Уверенность'
                        }
                    )
                    
                    # Quick add options
                    st.subheader("Быстрое добавление")
                    st.info("Отметьте предложения, которые хотите добавить:")
                    
                    selected_suggestions = []
                    for idx, suggestion in enumerate(suggestions):
                        if suggestion['confidence'] in ['High', 'Medium']:  # Only show high/medium confidence
                            col1, col2 = st.columns([1, 4])
                            with col1:
                                if st.checkbox("", key=f"suggest_{idx}"):
                                    selected_suggestions.append(suggestion)
                            with col2:
                                st.write(f"**{suggestion['wb_category']}** ↔ **{suggestion['oz_category']}** (сходство: {suggestion['similarity']:.2f})")
                    
                    if selected_suggestions:
                        if st.button("➕ Добавить выбранные соответствия", key="add_selected"):
                            success_count = 0
                            for suggestion in selected_suggestions:
                                if add_category_mapping(conn, suggestion['wb_category'], suggestion['oz_category'], f"Автопредложение (сходство: {suggestion['similarity']:.2f})"):
                                    success_count += 1
                            
                            if success_count > 0:
                                st.success(f"Добавлено {success_count} соответствий")
                                st.rerun()
                else:
                    st.info("Предложений не найдено. Попробуйте уменьшить порог сходства.")
    
    with mgmt_tab3:
        st.subheader("Существующие соответствия")
        
        if mappings_df.empty:
            st.info("Соответствия категорий еще не добавлены")
        else:
            # Search and filter
            search_term = st.text_input(
                "🔍 Поиск по категориям:",
                help="Введите часть названия категории WB или Ozon"
            )
            
            if search_term:
                filtered_df = mappings_df[
                    mappings_df['wb_category'].str.contains(search_term, case=False, na=False) |
                    mappings_df['oz_category'].str.contains(search_term, case=False, na=False)
                ]
            else:
                filtered_df = mappings_df
            
            st.write(f"Показано: {len(filtered_df)} из {len(mappings_df)} соответствий")
            
            # Display mappings with delete option
            for idx, row in filtered_df.iterrows():
                with st.container():
                    col1, col2, col3, col4, col5 = st.columns([3, 1, 3, 3, 1])
                    
                    with col1:
                        st.text(row['wb_category'])
                    
                    with col2:
                        st.text("↔")
                        
                    with col3:
                        st.text(row['oz_category'])
                    
                    with col4:
                        if row['notes']:
                            st.caption(f"💬 {row['notes']}")
                    
                    with col5:
                        if st.button("🗑️", key=f"delete_{row['id']}", help="Удалить соответствие"):
                            if delete_category_mapping(conn, row['id']):
                                st.rerun()
                    
                    st.divider()
    
    with mgmt_tab4:
        st.subheader("Импорт и экспорт соответствий")
        
        # Export section
        st.write("**📥 Экспорт**")
        col1, col2 = st.columns(2)
        
        with col1:
            include_stats = st.checkbox("Включить статистику по товарам", value=True)
        
        with col2:
            if st.button("📥 Экспортировать в CSV"):
                csv_content = export_category_mappings_to_csv(conn, include_stats)
                if csv_content:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"category_mappings_{timestamp}.csv"
                    
                    st.download_button(
                        label="Скачать CSV файл",
                        data=csv_content,
                        file_name=filename,
                        mime="text/csv"
                    )
        
        st.divider()
        
        # Import section
        st.write("**📤 Импорт**")
        st.info("Загрузите CSV файл с колонками: wb_category, oz_category, notes (опционально)")
        
        uploaded_file = st.file_uploader(
            "Выберите CSV файл:",
            type=['csv'],
            key="bulk_import_csv"
        )
        
        if uploaded_file is not None:
            try:
                import_df = pd.read_csv(uploaded_file)
                
                st.write("**Предпросмотр данных:**")
                st.dataframe(import_df.head(10), use_container_width=True)
                
                required_columns = ['wb_category', 'oz_category']
                missing_columns = [col for col in required_columns if col not in import_df.columns]
                
                if missing_columns:
                    st.error(f"❌ Отсутствуют обязательные колонки: {missing_columns}")
                else:
                    st.success("✅ Структура файла корректна")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Строк для импорта", len(import_df))
                    with col2:
                        valid_rows = len(import_df.dropna(subset=required_columns))
                        st.metric("Валидных строк", valid_rows)
                    
                    if st.button("🚀 Импортировать соответствия"):
                        with st.spinner("Импорт данных..."):
                            csv_content = uploaded_file.getvalue().decode('utf-8')
                            import_stats = import_category_mappings_from_csv(conn, csv_content)
                            
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Всего строк", import_stats['total_rows'])
                            with col2:
                                st.metric("Импортировано", import_stats['successful_imports'])
                            with col3:
                                st.metric("Пропущено (дубли)", import_stats['skipped_existing'])
                            with col4:
                                st.metric("Ошибок", import_stats['errors'])
                            
                            if import_stats['successful_imports'] > 0:
                                st.success(f"✅ Успешно импортировано {import_stats['successful_imports']} соответствий")
                                st.rerun()
                            else:
                                st.warning("⚠️ Новые соответствия не добавлены")
                        
            except Exception as e:
                st.error(f"Ошибка при чтении файла: {e}")
    
with tab3:
    st.header("📊 Статистика и аналитика")
    
    mappings_df = get_category_mappings(conn)
    usage_stats = get_category_usage_stats(conn)
    unmapped = get_unmapped_categories(conn)
    
    # Overview metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Всего соответствий", len(mappings_df))
    
    with col2:
        mapped_wb = mappings_df['wb_category'].nunique() if not mappings_df.empty else 0
        total_wb = len(get_unique_wb_categories(conn))
        st.metric("Покрытие WB", f"{mapped_wb}/{total_wb}", 
                 delta=f"{mapped_wb/total_wb*100:.1f}%" if total_wb > 0 else "0%")
    
    with col3:
        mapped_oz = mappings_df['oz_category'].nunique() if not mappings_df.empty else 0
        total_oz = len(get_unique_oz_categories(conn))
        st.metric("Покрытие Ozon", f"{mapped_oz}/{total_oz}",
                 delta=f"{mapped_oz/total_oz*100:.1f}%" if total_oz > 0 else "0%")
    
    with col4:
        unmapped_total = len(unmapped['wb']) + len(unmapped['oz'])
        st.metric("Несопоставленных", unmapped_total)
    
    if not mappings_df.empty:
        # Recent additions
        st.subheader("Недавно добавленные соответствия")
        recent_mappings = mappings_df.sort_values('created_at', ascending=False).head(10)
        st.dataframe(
            recent_mappings[['wb_category', 'oz_category', 'created_at']],
            use_container_width=True,
            hide_index=True,
            column_config={
                'wb_category': 'Категория WB',
                'oz_category': 'Категория Ozon',
                'created_at': st.column_config.DatetimeColumn(
                    'Дата создания',
                    format="DD.MM.YYYY HH:mm"
                )
            }
        )
        
        # Show unmapped categories
        if unmapped['wb'] or unmapped['oz']:
            st.subheader("⚠️ Несопоставленные категории")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**Wildberries ({len(unmapped['wb'])} категорий):**")
                if unmapped['wb']:
                    for category in unmapped['wb'][:10]:  # Show first 10
                        st.text(f"• {category}")
                    if len(unmapped['wb']) > 10:
                        st.caption(f"... и еще {len(unmapped['wb']) - 10} категорий")
                else:
                    st.success("✅ Все категории WB сопоставлены")
            
            with col2:
                st.write(f"**Ozon ({len(unmapped['oz'])} категорий):**")
                if unmapped['oz']:
                    for category in unmapped['oz'][:10]:  # Show first 10
                        st.text(f"• {category}")
                    if len(unmapped['oz']) > 10:
                        st.caption(f"... и еще {len(unmapped['oz']) - 10} категорий")
                else:
                    st.success("✅ Все категории Ozon сопоставлены")
        
        # Category usage statistics
        if not usage_stats['wb'].empty or not usage_stats['oz'].empty:
            st.subheader("📈 Использование категорий")
            
            col1, col2 = st.columns(2)
            
            with col1:
                if not usage_stats['wb'].empty:
                    st.write("**Топ категории WB по количеству товаров:**")
                    wb_top = usage_stats['wb'].head(10)
                    st.dataframe(
                        wb_top[['wb_category', 'unique_skus']],
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            'wb_category': 'Категория',
                            'unique_skus': 'Количество SKU'
                        }
                    )
            
            with col2:
                if not usage_stats['oz'].empty:
                    st.write("**Топ категории Ozon по количеству товаров:**")
                    oz_top = usage_stats['oz'].head(10)
                    st.dataframe(
                        oz_top[['oz_category', 'unique_skus']],
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            'oz_category': 'Категория',
                            'unique_skus': 'Количество SKU'
                        }
                    )
    else:
        st.info("Статистика будет доступна после добавления соответствий категорий")

# Connection will be closed automatically when the script ends
# No need to explicitly close it as it may interfere with cached operations 