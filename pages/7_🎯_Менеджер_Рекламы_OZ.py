"""
Streamlit page for Ozon advertising campaign management.

This page allows users to:
- Input Wildberries SKUs and find linked Ozon SKUs via common barcodes
- View comprehensive product data including vendor codes, Punta reference data, total stock/orders
- Select suitable Ozon SKUs for advertising campaigns based on comprehensive criteria
"""
import streamlit as st
from utils.db_connection import connect_db
from utils.db_search_helpers import get_normalized_wb_barcodes, get_ozon_barcodes_and_identifiers
import pandas as pd
from datetime import datetime, timedelta

st.set_page_config(page_title="Ozon RK Manager - Marketplace Analyzer", layout="wide")
st.title("🎯 Ozon RK Manager - Подбор артикулов для рекламы")
st.markdown("---")

# --- Database Connection ---
@st.cache_resource
def get_connection():
    conn = connect_db()
    if not conn:
        st.error("Database not connected. Please configure the database in Settings.")
        if st.button("Go to Settings", key="db_settings_button_rk"):
            st.switch_page("pages/3_Settings.py")
        st.stop()
    return conn

conn = get_connection()

# --- New Helper Functions ---

def convert_material_to_short(material: str) -> str:
    """
    Converts material description to short format according to business rules.
    
    Args:
        material: Original material description
        
    Returns:
        Shortened material code
    """
    if not material or (hasattr(pd, 'isna') and pd.isna(material)):
        return ""
    
    material_lower = str(material).lower().strip()
    
    if material_lower.startswith('н'):
        return "НК"
    elif material_lower.startswith('и'):
        return "ИК"
    elif material_lower.startswith('т'):
        return "Т"
    else:
        return str(material)

def get_ozon_product_details(db_conn, oz_sku_list: list[str]) -> pd.DataFrame:
    """
    Gets detailed Ozon product information including vendor codes and prices.
    
    Args:
        db_conn: Database connection
        oz_sku_list: List of Ozon SKUs
        
    Returns:
        DataFrame with oz_sku, oz_vendor_code, oz_actual_price, oz_fbo_stock
    """
    if not oz_sku_list:
        return pd.DataFrame()
    
    try:
        query = f"""
        SELECT 
            oz_sku,
            oz_vendor_code,
            COALESCE(oz_actual_price, 0) as oz_actual_price,
            COALESCE(oz_fbo_stock, 0) as oz_fbo_stock
        FROM oz_products 
        WHERE oz_sku IN ({', '.join(['?'] * len(oz_sku_list))})
        """
        
        details_df = db_conn.execute(query, oz_sku_list).fetchdf()
        details_df['oz_sku'] = details_df['oz_sku'].astype(str)
        return details_df
        
    except Exception as e:
        st.error(f"Ошибка при получении данных Ozon продуктов: {e}")
        return pd.DataFrame()

def get_oz_product_types(db_conn, oz_vendor_codes: list[str]) -> pd.DataFrame:
    """
    Gets product types from oz_category_products table for Ozon vendor codes.
    
    Args:
        db_conn: Database connection
        oz_vendor_codes: List of Ozon vendor codes
        
    Returns:
        DataFrame with oz_vendor_code, product_type
    """
    if not oz_vendor_codes:
        return pd.DataFrame()
    
    try:
        # Проверяем наличие таблицы oz_category_products
        table_check_query = "SELECT name FROM pragma_table_info('oz_category_products')"
        try:
            table_info = db_conn.execute(table_check_query).fetchdf()
            if table_info.empty:
                st.info("Таблица oz_category_products не найдена")
                return pd.DataFrame()
        except:
            st.info("Таблица oz_category_products не найдена")
            return pd.DataFrame()
        
        # Убираем пустые значения и дубликаты из vendor codes
        clean_vendor_codes = [str(vc).strip() for vc in oz_vendor_codes if str(vc).strip()]
        if not clean_vendor_codes:
            return pd.DataFrame()
        
        # Получаем типы продуктов из oz_category_products
        query = f"""
        SELECT DISTINCT
            oz_vendor_code,
            type as product_type
        FROM oz_category_products 
        WHERE oz_vendor_code IN ({', '.join(['?'] * len(clean_vendor_codes))})
            AND NULLIF(TRIM(type), '') IS NOT NULL
        """
        
        types_df = db_conn.execute(query, clean_vendor_codes).fetchdf()
        types_df['oz_vendor_code'] = types_df['oz_vendor_code'].astype(str)
        
        return types_df
        
    except Exception as e:
        st.error(f"Ошибка при получении типов продуктов из oz_category_products: {e}")
        return pd.DataFrame()

def get_punta_data_for_rk(db_conn, wb_sku_list: list[str]) -> pd.DataFrame:
    """
    Gets Punta reference data for WB SKUs with specific columns for RK manager.
    
    ИСПРАВЛЕНИЕ: Теперь возвращает только gender, season, material из Punta.
    Поле wb_object (предмет) теперь получается из oz_category_products.type.
    
    Args:
        db_conn: Database connection
        wb_sku_list: List of WB SKUs
        
    Returns:
        DataFrame with wb_sku, gender, season, material (без wb_object)
    """
    if not wb_sku_list:
        return pd.DataFrame()
    
    try:
        # Convert wb_sku_list to integers for proper matching
        wb_skus_int = []
        for wb_sku in wb_sku_list:
            try:
                wb_skus_int.append(int(wb_sku))
            except (ValueError, TypeError):
                continue
        
        if not wb_skus_int:
            return pd.DataFrame()
        
        # Check which columns exist in punta_table
        columns_query = "PRAGMA table_info(punta_table)"
        try:
            columns_info = db_conn.execute(columns_query).fetchdf()
            available_columns = columns_info['name'].tolist()
        except:
            st.info("Таблица punta_table не найдена")
            return pd.DataFrame()
        
        # Build query for available columns
        target_columns = ['wb_sku', 'gender', 'season', 'material', 'wb_object']
        existing_columns = [col for col in target_columns if col in available_columns]
        
        if len(existing_columns) <= 1:  # Only wb_sku exists
            return pd.DataFrame()
        
        query = f"""
        WITH first_occurrences AS (
            SELECT {', '.join(f'"{col}"' for col in existing_columns)},
                   ROW_NUMBER() OVER (PARTITION BY wb_sku ORDER BY ROWID) as rn
            FROM punta_table 
            WHERE wb_sku IN ({', '.join(['?'] * len(wb_skus_int))})
        )
        SELECT {', '.join(f'"{col}"' for col in existing_columns)}
        FROM first_occurrences 
        WHERE rn = 1
        """
        
        punta_df = db_conn.execute(query, wb_skus_int).fetchdf()
        punta_df['wb_sku'] = punta_df['wb_sku'].astype(str)
        
        # Apply material conversion if material column exists
        if 'material' in punta_df.columns:
            punta_df['material'] = punta_df['material'].apply(convert_material_to_short)
        
        # ИСПРАВЛЕНИЕ: Убираем wb_object из списка возвращаемых колонок
        # wb_object теперь получается из oz_category_products.type
        target_columns_without_object = ['wb_sku', 'gender', 'season', 'material']
        
        # Fill missing columns with empty strings
        for col in target_columns_without_object:
            if col not in punta_df.columns:
                punta_df[col] = ""
        
        return punta_df[target_columns_without_object]
        
    except Exception as e:
        st.error(f"Ошибка при получении данных Punta: {e}")
        return pd.DataFrame()

def calculate_total_stock_by_wb_sku(db_conn, wb_sku_list: list[str]) -> pd.DataFrame:
    """
    Calculates total stock for each WB SKU across all linked Ozon SKUs.
    
    Args:
        db_conn: Database connection
        wb_sku_list: List of WB SKUs
        
    Returns:
        DataFrame with wb_sku, total_stock_wb_sku
    """
    if not wb_sku_list:
        return pd.DataFrame()
    
    try:
        # Get all WB-Ozon mappings
        wb_barcodes_df = get_normalized_wb_barcodes(db_conn, wb_skus=wb_sku_list)
        if wb_barcodes_df.empty:
            return pd.DataFrame()
        
        oz_barcodes_ids_df = get_ozon_barcodes_and_identifiers(db_conn)
        if oz_barcodes_ids_df.empty:
            return pd.DataFrame()
        
        # Prepare data for merge
        wb_barcodes_df = wb_barcodes_df.rename(columns={'individual_barcode_wb': 'barcode'})
        oz_barcodes_ids_df = oz_barcodes_ids_df.rename(columns={'oz_barcode': 'barcode'})
        
        # ИСПРАВЛЕНИЕ: Нормализуем данные для корректного связывания без дублей
        # Приводим штрихкоды к строковому типу и убираем пробелы (строка 191)
        wb_barcodes_df['barcode'] = wb_barcodes_df['barcode'].astype(str).str.strip()
        oz_barcodes_ids_df['barcode'] = oz_barcodes_ids_df['barcode'].astype(str).str.strip()
        wb_barcodes_df['wb_sku'] = wb_barcodes_df['wb_sku'].astype(str)
        oz_barcodes_ids_df['oz_sku'] = oz_barcodes_ids_df['oz_sku'].astype(str)
        
        # Remove empty barcodes
        wb_barcodes_df = wb_barcodes_df[wb_barcodes_df['barcode'] != ''].drop_duplicates()
        oz_barcodes_ids_df = oz_barcodes_ids_df[oz_barcodes_ids_df['barcode'] != ''].drop_duplicates()
        
        # Merge to find common barcodes
        merged_df = pd.merge(wb_barcodes_df, oz_barcodes_ids_df, on='barcode', how='inner')
        
        if merged_df.empty:
            return pd.DataFrame()
        
        # ИСПРАВЛЕНИЕ: Удаляем дубликаты по wb_sku-oz_sku связкам ПЕРЕД подсчетом остатков
        # Это критично для корректного расчета остатков
        merged_df = merged_df.drop_duplicates(subset=['wb_sku', 'oz_sku'], keep='first')
        
        # Get stock data for found Ozon SKUs
        oz_skus_for_query = list(merged_df['oz_sku'].unique())
        
        stock_query = f"""
        SELECT 
            oz_sku,
            COALESCE(oz_fbo_stock, 0) as oz_fbo_stock
        FROM oz_products 
        WHERE oz_sku IN ({', '.join(['?'] * len(oz_skus_for_query))})
        """
        
        stock_df = db_conn.execute(stock_query, oz_skus_for_query).fetchdf()
        stock_df['oz_sku'] = stock_df['oz_sku'].astype(str)
        
        # Merge stock data with mappings
        merged_with_stock_df = pd.merge(merged_df, stock_df, on='oz_sku', how='left')
        merged_with_stock_df['oz_fbo_stock'] = merged_with_stock_df['oz_fbo_stock'].fillna(0)
        
        # ИСПРАВЛЕНИЕ: Дополнительная проверка - удаляем дубликаты перед агрегацией
        merged_with_stock_df = merged_with_stock_df.drop_duplicates(subset=['wb_sku', 'oz_sku'], keep='first')
        
        # Aggregate by wb_sku
        total_stock_df = merged_with_stock_df.groupby('wb_sku')['oz_fbo_stock'].sum().reset_index()
        total_stock_df.columns = ['wb_sku', 'total_stock_wb_sku']
        
        return total_stock_df
        
    except Exception as e:
        st.error(f"Ошибка при расчете общего остатка: {e}")
        return pd.DataFrame()

def calculate_total_orders_by_wb_sku(db_conn, wb_sku_list: list[str], days_back: int = 14) -> pd.DataFrame:
    """
    Calculates total orders for each WB SKU across all linked Ozon SKUs for specified period.
    
    Args:
        db_conn: Database connection
        wb_sku_list: List of WB SKUs
        days_back: Number of days to look back
        
    Returns:
        DataFrame with wb_sku, total_orders_wb_sku
    """
    if not wb_sku_list:
        return pd.DataFrame()
    
    try:
        # Get all WB-Ozon mappings
        wb_barcodes_df = get_normalized_wb_barcodes(db_conn, wb_skus=wb_sku_list)
        if wb_barcodes_df.empty:
            return pd.DataFrame()
        
        oz_barcodes_ids_df = get_ozon_barcodes_and_identifiers(db_conn)
        if oz_barcodes_ids_df.empty:
            return pd.DataFrame()
        
        # Prepare data for merge
        wb_barcodes_df = wb_barcodes_df.rename(columns={'individual_barcode_wb': 'barcode'})
        oz_barcodes_ids_df = oz_barcodes_ids_df.rename(columns={'oz_barcode': 'barcode'})
        
        # Ensure barcode consistency
        wb_barcodes_df['barcode'] = wb_barcodes_df['barcode'].astype(str).str.strip()
        oz_barcodes_ids_df['barcode'] = oz_barcodes_ids_df['barcode'].astype(str).str.strip()
        wb_barcodes_df['wb_sku'] = wb_barcodes_df['wb_sku'].astype(str)
        oz_barcodes_ids_df['oz_sku'] = oz_barcodes_ids_df['oz_sku'].astype(str)
        
        # Remove empty barcodes
        wb_barcodes_df = wb_barcodes_df[wb_barcodes_df['barcode'] != ''].drop_duplicates()
        oz_barcodes_ids_df = oz_barcodes_ids_df[oz_barcodes_ids_df['barcode'] != ''].drop_duplicates()
        
        # Merge to find common barcodes
        merged_df = pd.merge(wb_barcodes_df, oz_barcodes_ids_df, on='barcode', how='inner')
        
        if merged_df.empty:
            return pd.DataFrame()
        
        # ИСПРАВЛЕНИЕ: Удаляем дубликаты по wb_sku-oz_sku связкам ПЕРЕД подсчетом заказов
        # Это критично для корректного расчета заказов
        merged_df = merged_df.drop_duplicates(subset=['wb_sku', 'oz_sku'], keep='first')
        
        # Get order data for found Ozon SKUs
        oz_skus_for_query = list(merged_df['oz_sku'].unique())
        
        # Calculate dates
        today = datetime.now()
        start_date = (today - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        orders_query = f"""
        SELECT 
            oz_sku,
            COUNT(*) as order_count
        FROM oz_orders
        WHERE oz_sku IN ({', '.join(['?'] * len(oz_skus_for_query))})
            AND oz_accepted_date >= ?
            AND order_status != 'Отменён'
        GROUP BY oz_sku
        """
        
        orders_df = db_conn.execute(orders_query, oz_skus_for_query + [start_date]).fetchdf()
        orders_df['oz_sku'] = orders_df['oz_sku'].astype(str)
        
        # Merge order data with mappings
        merged_with_orders_df = pd.merge(merged_df, orders_df, on='oz_sku', how='left')
        merged_with_orders_df['order_count'] = merged_with_orders_df['order_count'].fillna(0)
        
        # ИСПРАВЛЕНИЕ: Дополнительная проверка - удаляем дубликаты перед агрегацией
        merged_with_orders_df = merged_with_orders_df.drop_duplicates(subset=['wb_sku', 'oz_sku'], keep='first')
        
        # Aggregate by wb_sku
        total_orders_df = merged_with_orders_df.groupby('wb_sku')['order_count'].sum().reset_index()
        total_orders_df.columns = ['wb_sku', 'total_orders_wb_sku']
        
        return total_orders_df
        
    except Exception as e:
        st.error(f"Ошибка при расчете общих заказов: {e}")
        return pd.DataFrame()

# --- Helper Functions ---

def get_linked_ozon_skus_with_details(db_conn, wb_sku_list: list[str]) -> pd.DataFrame:
    """
    For a list of WB SKUs, finds all linked Ozon SKUs via common barcodes
    and enriches with comprehensive product data for advertising campaign selection.
    
    ИСПРАВЛЕНИЕ: Поле wb_object (предмет) теперь получается из oz_category_products.type
    по oz_vendor_code вместо данных Punta.
    
    Returns DataFrame with columns:
    - group_num: Group number (starting from 1)
    - wb_sku: WB SKU
    - oz_sku: Linked Ozon SKU
    - oz_vendor_code: Ozon vendor code
    - oz_fbo_stock: Individual stock level
    - oz_orders_14: Individual orders in last 14 days
    - oz_actual_price: Current price in Ozon
    - gender: Gender from Punta
    - season: Season from Punta
    - material: Material from Punta
    - wb_object: Object type from oz_category_products.type (по oz_vendor_code)
    - total_stock_wb_sku: Total stock for entire WB SKU
    - total_orders_wb_sku: Total orders for entire WB SKU (14 days)
    """
    if not wb_sku_list:
        return pd.DataFrame()

    # Ensure WB SKUs are strings for helper functions
    wb_sku_list_str = [str(sku) for sku in wb_sku_list]

    # Use centralized linker for WB-Ozon connections
    from utils.cross_marketplace_linker import CrossMarketplaceLinker
    linker = CrossMarketplaceLinker(db_conn)
    
    # Get basic links with product details
    linked_df = linker.get_extended_links(wb_sku_list_str, include_product_details=True)
    if linked_df.empty:
        st.info("Не найдено совпадений Ozon SKU для указанных WB SKU по штрихкодам.")
        return pd.DataFrame()

    # ИСПРАВЛЕНИЕ: Получаем уникальные пары WB-Ozon SKU с более агрессивной дедупликацией
    sku_pairs_df = linked_df[['wb_sku', 'oz_sku']].drop_duplicates(subset=['wb_sku', 'oz_sku'], keep='first')
    
    # Add group numbers
    group_mapping = {}
    current_group = 1
    for wb_sku in sku_pairs_df['wb_sku'].unique():
        group_mapping[wb_sku] = current_group
        current_group += 1
    
    sku_pairs_df['group_num'] = sku_pairs_df['wb_sku'].map(group_mapping)
    
    # Get comprehensive Ozon product details (some already in linked_df)
    oz_skus_for_query = list(sku_pairs_df['oz_sku'].unique())
    
    # Extract existing product details from linker results
    if 'oz_actual_price' in linked_df.columns:
        # Use data from centralized linker (already includes product details)
        ozon_details_df = linked_df[['oz_sku', 'oz_vendor_code', 'oz_actual_price', 'oz_fbo_stock']].drop_duplicates()
    else:
        # Fallback to separate query
        ozon_details_df = get_ozon_product_details(db_conn, oz_skus_for_query)
    
    # Get individual orders for last 14 days
    orders_df = get_ozon_orders_14_days(db_conn, oz_skus_for_query)
    
    # Get Punta reference data (for gender, season, material)
    unique_wb_skus = list(sku_pairs_df['wb_sku'].unique())
    punta_df = get_punta_data_for_rk(db_conn, unique_wb_skus)
    
    # ИСПРАВЛЕНИЕ: Получаем типы продуктов (предметы) из oz_category_products
    # Используем oz_vendor_code из Ozon продуктов вместо wb_object из Punta
    unique_oz_vendor_codes = []
    if not ozon_details_df.empty and 'oz_vendor_code' in ozon_details_df.columns:
        unique_oz_vendor_codes = [code for code in ozon_details_df['oz_vendor_code'].unique() if str(code).strip()]
    
    product_types_df = get_oz_product_types(db_conn, unique_oz_vendor_codes)
    
    # Get total stock by WB SKU
    total_stock_df = calculate_total_stock_by_wb_sku(db_conn, unique_wb_skus)
    
    # Get total orders by WB SKU
    total_orders_df = calculate_total_orders_by_wb_sku(db_conn, unique_wb_skus, days_back=14)
    
    # Merge all data step by step
    result_df = sku_pairs_df.copy()
    
    # Merge Ozon product details
    if not ozon_details_df.empty:
        result_df = pd.merge(result_df, ozon_details_df, on='oz_sku', how='left')
    else:
        result_df['oz_vendor_code'] = ""
        result_df['oz_actual_price'] = 0
        result_df['oz_fbo_stock'] = 0
    
    # Merge individual order data
    if not orders_df.empty:
        result_df = pd.merge(result_df, orders_df, on='oz_sku', how='left')
    else:
        result_df['oz_orders_14'] = 0
    
    # Merge Punta data (gender, season, material)
    if not punta_df.empty:
        result_df = pd.merge(result_df, punta_df, on='wb_sku', how='left')
    else:
        result_df['gender'] = ""
        result_df['season'] = ""
        result_df['material'] = ""
    
    # ИСПРАВЛЕНИЕ: Объединяем данные о типах продуктов из oz_category_products
    # Получаем "Предмет" по oz_vendor_code вместо wb_object из Punta
    if not product_types_df.empty:
        result_df = pd.merge(result_df, product_types_df, on='oz_vendor_code', how='left')
        result_df['wb_object'] = result_df['product_type'].fillna("")
        result_df = result_df.drop('product_type', axis=1)  # Убираем промежуточную колонку
    else:
        result_df['wb_object'] = ""
    
    # Merge total stock data
    if not total_stock_df.empty:
        result_df = pd.merge(result_df, total_stock_df, on='wb_sku', how='left')
    else:
        result_df['total_stock_wb_sku'] = 0
    
    # Merge total orders data
    if not total_orders_df.empty:
        result_df = pd.merge(result_df, total_orders_df, on='wb_sku', how='left')
    else:
        result_df['total_orders_wb_sku'] = 0
    
    # Fill NaN values and ensure proper data types
    result_df['oz_fbo_stock'] = result_df['oz_fbo_stock'].fillna(0).astype(int)
    result_df['oz_orders_14'] = result_df['oz_orders_14'].fillna(0).astype(int)
    result_df['oz_actual_price'] = result_df['oz_actual_price'].fillna(0).round().astype(int)  # Round price to integer
    result_df['total_stock_wb_sku'] = result_df['total_stock_wb_sku'].fillna(0).astype(int)
    result_df['total_orders_wb_sku'] = result_df['total_orders_wb_sku'].fillna(0).astype(int)
    
    # Fill string fields
    for col in ['oz_vendor_code', 'gender', 'season', 'material', 'wb_object']:
        result_df[col] = result_df[col].fillna("")
    
    # Sort by group number and then by orders descending
    result_df = result_df.sort_values(['group_num', 'oz_orders_14'], ascending=[True, False])
    
    return result_df

def get_ozon_orders_14_days(db_conn, oz_sku_list: list[str]) -> pd.DataFrame:
    """
    Calculate orders for given Ozon SKUs for the last 14 days.
    Returns DataFrame with oz_sku and oz_orders_14 columns.
    """
    if not oz_sku_list:
        return pd.DataFrame()
    
    # Calculate dates
    today = datetime.now()
    fourteen_days_ago = today - timedelta(days=14)
    
    try:
        query = f"""
        SELECT 
            oz_sku,
            COUNT(*) as oz_orders_14
        FROM oz_orders 
        WHERE oz_sku IN ({', '.join(['?'] * len(oz_sku_list))})
            AND oz_accepted_date >= ?
            AND order_status != 'Отменён'
        GROUP BY oz_sku
        """
        
        orders_df = db_conn.execute(
            query, 
            oz_sku_list + [fourteen_days_ago.strftime('%Y-%m-%d')]
        ).fetchdf()
        
        orders_df['oz_sku'] = orders_df['oz_sku'].astype(str)
        return orders_df
        
    except Exception as e:
        st.error(f"Ошибка при получении данных о заказах: {e}")
        return pd.DataFrame()

def select_advertising_candidates(df: pd.DataFrame, min_stock: int = 20, min_candidates: int = 1, max_candidates: int = 5) -> pd.DataFrame:
    """
    Select top candidates for advertising based on order volume and stock within each group.
    Now preserves all the comprehensive data columns including Punta reference data.
    
    Args:
        df: DataFrame with all WB-Ozon SKU mappings and comprehensive data
        min_stock: Minimum required stock level (individual oz_fbo_stock)
        min_candidates: Minimum number of candidates required per group (groups with fewer are excluded)
        max_candidates: Maximum number of candidates to select PER GROUP
    
    Returns:
        DataFrame with selected candidates including all comprehensive data columns
    """
    if df.empty:
        return pd.DataFrame()
    
    all_selected = []
    
    # Process each group separately
    for group_num in df['group_num'].unique():
        group_df = df[df['group_num'] == group_num].copy()
        
        # Filter by minimum stock within this group (individual stock)
        filtered_group_df = group_df[group_df['oz_fbo_stock'] >= min_stock].copy()
        
        # Check if group meets minimum candidates requirement
        if len(filtered_group_df) < min_candidates:
            continue  # Skip this entire group - doesn't meet minimum requirement
        
        # Sort by orders descending and take top candidates for this group
        selected_group_df = filtered_group_df.nlargest(max_candidates, 'oz_orders_14')
        
        all_selected.append(selected_group_df)
    
    if not all_selected:
        return pd.DataFrame()
    
    # Combine all selected candidates from all groups
    result_df = pd.concat(all_selected, ignore_index=True)
    
    # Sort final result by group number and then by orders descending
    result_df = result_df.sort_values(['group_num', 'oz_orders_14'], ascending=[True, False])
    
    return result_df

# --- UI Elements ---
st.subheader("🔍 Поиск связанных артикулов")

col1, col2 = st.columns([2, 1])

with col1:
    wb_skus_input = st.text_area(
        "Введите артикулы WB для поиска связанных артикулов Ozon:",
        height=100,
        help="Например: 12345 67890 98765",
        placeholder="Введите WB SKU через пробел..."
    )

with col2:
    st.markdown("##### Параметры отбора для рекламы")
    min_stock_setting = st.number_input(
        "Минимальный остаток (шт):",
        min_value=0,
        max_value=1000,
        value=20,
        step=1
    )
    
    col2a, col2b = st.columns(2)
    with col2a:
        min_candidates_setting = st.number_input(
            "Минимум кандидатов:",
            min_value=1,
            max_value=20,
            value=1,
            step=1,
            help="Минимальное количество подходящих артикулов в группе. Если меньше - группа исключается."
        )
    
    with col2b:
        max_candidates_setting = st.number_input(
            "Максимум кандидатов:",
            min_value=1,
            max_value=20,
            value=5,
            step=1,
            help="Максимальное количество кандидатов для отбора из группы."
        )

st.markdown("---")

# Initialize session state for storing results
if 'rk_search_results' not in st.session_state:
    st.session_state.rk_search_results = pd.DataFrame()

# Search and select button
if st.button("🎯 Найти и отобрать кандидатов для рекламы", type="primary"):
    if not wb_skus_input.strip():
        st.warning("Пожалуйста, введите артикулы WB для поиска.")
    else:
        wb_sku_list = wb_skus_input.strip().split()
        
        with st.spinner("Поиск связанных артикулов и отбор кандидатов..."):
            # Get all linked SKUs with comprehensive data
            all_results_df = get_linked_ozon_skus_with_details(conn, wb_sku_list)
            
            if not all_results_df.empty:
                # Apply selection criteria immediately
                selected_df = select_advertising_candidates(
                    all_results_df, 
                    min_stock=min_stock_setting,
                    min_candidates=min_candidates_setting,
                    max_candidates=max_candidates_setting
                )
                st.session_state.rk_search_results = selected_df
            else:
                st.session_state.rk_search_results = pd.DataFrame()

# Display selected candidates if available
if not st.session_state.rk_search_results.empty:
    selected_df = st.session_state.rk_search_results
    
    st.subheader("🎯 Отобранные кандидаты для рекламы")
    
    # Summary statistics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Отобрано групп", selected_df['group_num'].nunique())
    with col2:
        st.metric("Всего кандидатов", len(selected_df))
    with col3:
        # Calculate unique stock per model (avoid duplicating total_stock_wb_sku for each size)
        unique_stock_per_model = selected_df.groupby('wb_sku')['total_stock_wb_sku'].first().sum()
        st.metric("Общий остаток моделей", unique_stock_per_model)
    with col4:
        # Calculate unique orders per model (avoid duplicating total_orders_wb_sku for each size)
        unique_orders_per_model = selected_df.groupby('wb_sku')['total_orders_wb_sku'].first().sum()
        st.metric("Общие заказы моделей (14 дней)", unique_orders_per_model)
    
    # Display comprehensive candidate table
    st.markdown("##### 🎯 Детальная информация по кандидатам:")
    
    display_df = selected_df.copy()
    
    # Rename columns for display
    display_df = display_df.rename(columns={
        'group_num': 'Группа',
        'wb_sku': 'WB SKU', 
        'oz_sku': 'Ozon SKU',
        'oz_vendor_code': 'Артикул OZ',
        'oz_fbo_stock': 'Остаток',
        'oz_orders_14': 'Заказов за 14 дней',
        'oz_actual_price': 'Цена, ₽',
        'gender': 'Пол',
        'season': 'Сезон',
        'material': 'Материал',
        'wb_object': 'Предмет',
        'total_stock_wb_sku': 'Остаток всей модели',
        'total_orders_wb_sku': 'Заказы всей модели (14 дней)'
    })
    
    # Add ranking within each group
    display_df['Рейтинг в группе'] = display_df.groupby('Группа').cumcount() + 1
    
    # Add margin placeholder
    display_df['% маржи'] = "скоро"
    
    # Reorder columns for better display
    column_order = [
        'Группа', 'WB SKU', 'Рейтинг в группе', 
        'Ozon SKU', 'Артикул OZ', 
        'Остаток', 'Заказов за 14 дней', 'Цена, ₽',
        'Пол', 'Сезон', 'Материал', 'Предмет',
        'Остаток всей модели', 'Заказы всей модели (14 дней)', '% маржи'
    ]
    
    display_df = display_df[column_order]
    
    # Style the dataframe to highlight by groups
    def highlight_by_group(row):
        if row['Рейтинг в группе'] == 1:
            return ['background-color: #e8f5e8'] * len(row)  # Green for #1 in group
        elif row['Рейтинг в группе'] <= 3:
            return ['background-color: #f0f8ff'] * len(row)  # Light blue for top 3
        else:
            return [''] * len(row)
    
    styled_df = display_df.style.apply(highlight_by_group, axis=1)
    st.dataframe(styled_df, use_container_width=True, hide_index=True)
    
    # Show applied criteria
    st.info(f"**Применённые критерии отбора:**\n"
            f"• Минимальный остаток: {min_stock_setting} шт (индивидуальный)\n"
            f"• Минимум кандидатов: {min_candidates_setting} из каждой группы\n"
            f"• Максимум кандидатов: до {max_candidates_setting} из каждой группы\n"
            f"• Сортировка: по убыванию заказов за 14 дней внутри каждой группы")
    
    # Export options
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📋 Копировать Ozon SKU для рекламы", use_container_width=True):
            ozon_skus_list = selected_df['oz_sku'].tolist()
            ozon_skus_text = ' '.join(ozon_skus_list)
            st.code(ozon_skus_text, language="text")
            st.success("Скопируйте Ozon SKU из поля выше для использования в рекламной системе")
    
    with col2:
        if st.button("📊 Показать сводку по группам", use_container_width=True):
            st.markdown("##### 📈 Сводка по группам:")
            
            # Create group summary
            group_summary = selected_df.groupby(['group_num', 'wb_sku']).agg({
                'oz_sku': 'count',
                'oz_fbo_stock': 'sum',
                'oz_orders_14': 'sum',
                'total_stock_wb_sku': 'first',  # This is same for all rows in group
                'total_orders_wb_sku': 'first'  # This is same for all rows in group
            }).rename(columns={
                'oz_sku': 'Кандидатов',
                'oz_fbo_stock': 'Остаток отобранных',
                'oz_orders_14': 'Заказы отобранных',
                'total_stock_wb_sku': 'Остаток всей модели',
                'total_orders_wb_sku': 'Заказы всей модели'
            }).reset_index()
            
            group_summary = group_summary.rename(columns={
                'group_num': 'Группа',
                'wb_sku': 'WB SKU'
            })
            
            st.dataframe(group_summary, use_container_width=True, hide_index=True)
    
    # Breakdown by groups
    with st.expander("📋 Разбивка Ozon SKU по группам"):
        for group_num in sorted(selected_df['group_num'].unique()):
            group_data = selected_df[selected_df['group_num'] == group_num]
            wb_sku = group_data['wb_sku'].iloc[0]
            group_skus = group_data['oz_sku'].tolist()
            st.write(f"**Группа {group_num} (WB SKU: {wb_sku}):** {' '.join(group_skus)}")

else:
    if 'rk_search_results' in st.session_state and st.session_state.rk_search_results is not None:
        # Check if we tried to search but got no results
        st.warning(f"❌ Не найдено кандидатов, соответствующих критериям:\n"
                  f"- Минимальный остаток: {min_stock_setting} шт\n"
                  f"- Минимум кандидатов: {min_candidates_setting} из каждой группы\n"
                  f"- Максимум кандидатов: до {max_candidates_setting} из каждой группы\n"
                  f"- Возможно, в группах недостаточно артикулов с нужным остатком\n"
                  f"- Попробуйте уменьшить минимальный остаток или минимум кандидатов")

# Instructions section
with st.expander("📖 Инструкция по использованию"):
    st.markdown("""
    ### Как использовать обновленный Ozon RK Manager:
    
    1. **Ввод данных**: Введите список WB SKU через пробел в текстовое поле
    
    2. **Настройка критериев отбора** (справа):
       - **Минимальный остаток**: товары с меньшим индивидуальным остатком не будут рассматриваться
       - **Минимум кандидатов**: минимальное количество подходящих артикулов в группе (если меньше - вся группа исключается)
       - **Максимум кандидатов**: максимальное количество артикулов для отбора из каждой группы
    
    3. **Поиск и отбор**: Нажмите "🎯 Найти и отобрать кандидатов для рекламы"
       - Система автоматически найдет все связанные Ozon SKU через общие штрихкоды
       - Загрузит comprehensive данные: артикулы, цены, остатки, справочные данные Punta
       - Применит критерии отбора и покажет только лучших кандидатов
    
    4. **Анализ результатов**: В таблице "Детальная информация по кандидатам" отображаются:
       - **Базовые данные**: Группа, WB SKU, Ozon SKU, Артикул OZ, Рейтинг в группе
       - **Остатки и заказы**: Индивидуальные и общие по всей модели
       - **Ценовая информация**: Текущая цена Ozon, будущая колонка "% маржи"
       - **Справочные данные Punta**: Пол, Сезон, Материал, Предмет
    
    5. **Экспорт результатов**:
       - **"Копировать Ozon SKU"**: получить список для вставки в рекламную систему
       - **"Показать сводку по группам"**: агрегированная статистика по WB SKU
       - **"Разбивка по группам"**: детальная разбивка Ozon SKU по группам
    
    ### Новые возможности и столбцы:
    
    #### 📊 Расширенная аналитика:
    - **Остаток всей модели**: суммарный остаток всех размеров WB SKU в Ozon
    - **Заказы всей модели**: суммарные заказы всех размеров за 14 дней
    - **Артикул OZ**: оригинальный артикул поставщика в системе Ozon
    - **Цена**: текущая цена с учетом скидки в Ozon
    
    #### 📚 Справочные данные Punta:
    - **Пол**: целевая аудитория (Мальчики/Девочки/Мужской/Женский)
    - **Сезон**: сезонность товара (Лето/Деми/Зима)
    - **Материал**: описание материалов изделия
    - **Предмет**: тип товара (Ботинки/Кроссовки/Сапоги и т.д.)
    
    #### 🎯 Улучшенная логика отбора:
    - **Автоматический отбор**: сразу показываются только лучшие кандидаты
    - **Цветовая индикация**: зеленый для #1 в группе, голубой для топ-3
    - **Comprehensive view**: вся необходимая информация в одной таблице
    
    ### Логика работы:
    - **Группировка**: Каждый WB SKU образует отдельную группу товаров
    - **Агрегация данных**: Автоматический расчет общих остатков и заказов по модели
    - **Enrichment**: Обогащение справочными данными из таблицы Punta
    - **Smart filtering**: Исключение групп с недостаточным количеством кандидатов
    - **Ranking**: Ранжирование по продуктивности (заказы за 14 дней)
    """)

# Remove the explicit conn.close() when using st.cache_resource for the connection 