"""
Streamlit page for Ozon advertising campaign management.

This page allows users to:
- Input Wildberries SKUs and find linked Ozon SKUs via common barcodes
- View stock levels and order statistics for found Ozon SKUs
- Select suitable Ozon SKUs for advertising campaigns based on order volume and stock
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

# --- Helper Functions ---

def get_linked_ozon_skus_with_details(db_conn, wb_sku_list: list[str]) -> pd.DataFrame:
    """
    For a list of WB SKUs, finds all linked Ozon SKUs via common barcodes
    and enriches with stock and order data.
    
    Returns DataFrame with columns:
    - group_num: Group number (starting from 1)
    - wb_sku: WB SKU
    - oz_sku: Linked Ozon SKU
    - oz_fbo_stock: Stock level from oz_products
    - oz_orders_14: Orders in last 14 days
    """
    if not wb_sku_list:
        return pd.DataFrame()

    # Ensure WB SKUs are strings for helper functions
    wb_sku_list_str = [str(sku) for sku in wb_sku_list]

    # Get WB barcodes
    wb_barcodes_df = get_normalized_wb_barcodes(db_conn, wb_skus=wb_sku_list_str)
    if wb_barcodes_df.empty:
        st.warning("Не найдены штрихкоды для указанных WB SKU.")
        return pd.DataFrame()

    # Get all Ozon barcodes to find matches
    oz_barcodes_ids_df = get_ozon_barcodes_and_identifiers(db_conn)
    if oz_barcodes_ids_df.empty:
        st.warning("Не удалось загрузить штрихкоды Ozon для сопоставления.")
        return pd.DataFrame()

    # Prepare data for merge
    wb_barcodes_df = wb_barcodes_df.rename(columns={'individual_barcode_wb': 'barcode'})
    oz_barcodes_ids_df = oz_barcodes_ids_df.rename(columns={'oz_barcode': 'barcode'})
    
    # Ensure barcode columns are strings and clean
    wb_barcodes_df['barcode'] = wb_barcodes_df['barcode'].astype(str).str.strip()
    oz_barcodes_ids_df['barcode'] = oz_barcodes_ids_df['barcode'].astype(str).str.strip()
    
    # Convert SKU columns to string for consistency
    wb_barcodes_df['wb_sku'] = wb_barcodes_df['wb_sku'].astype(str)
    oz_barcodes_ids_df['oz_sku'] = oz_barcodes_ids_df['oz_sku'].astype(str)
    
    # Remove empty barcodes and duplicates
    wb_barcodes_df = wb_barcodes_df[wb_barcodes_df['barcode'] != ''].drop_duplicates()
    oz_barcodes_ids_df = oz_barcodes_ids_df[oz_barcodes_ids_df['barcode'] != ''].drop_duplicates()

    # Merge to find common barcodes
    merged_df = pd.merge(wb_barcodes_df, oz_barcodes_ids_df, on='barcode', how='inner')

    if merged_df.empty:
        st.info("Не найдено совпадений Ozon SKU для указанных WB SKU по штрихкодам.")
        return pd.DataFrame()

    # Get unique WB-Ozon SKU pairs
    sku_pairs_df = merged_df[['wb_sku', 'oz_sku']].drop_duplicates()
    
    # Add group numbers
    group_mapping = {}
    current_group = 1
    for wb_sku in sku_pairs_df['wb_sku'].unique():
        group_mapping[wb_sku] = current_group
        current_group += 1
    
    sku_pairs_df['group_num'] = sku_pairs_df['wb_sku'].map(group_mapping)
    
    # Get stock data from oz_products
    oz_skus_for_query = list(sku_pairs_df['oz_sku'].unique())
    if oz_skus_for_query:
        try:
            stock_query = f"""
            SELECT 
                oz_sku,
                COALESCE(oz_fbo_stock, 0) as oz_fbo_stock
            FROM oz_products 
            WHERE oz_sku IN ({', '.join(['?'] * len(oz_skus_for_query))})
            """
            stock_df = db_conn.execute(stock_query, oz_skus_for_query).fetchdf()
            stock_df['oz_sku'] = stock_df['oz_sku'].astype(str)
        except Exception as e:
            st.error(f"Ошибка при получении данных об остатках: {e}")
            stock_df = pd.DataFrame()
    else:
        stock_df = pd.DataFrame()
    
    # Get order data for last 14 days
    orders_df = get_ozon_orders_14_days(db_conn, oz_skus_for_query)
    
    # Merge all data
    result_df = sku_pairs_df.copy()
    
    # Merge stock data
    if not stock_df.empty:
        result_df = pd.merge(result_df, stock_df, on='oz_sku', how='left')
    else:
        result_df['oz_fbo_stock'] = 0
    
    # Merge order data
    if not orders_df.empty:
        result_df = pd.merge(result_df, orders_df, on='oz_sku', how='left')
    else:
        result_df['oz_orders_14'] = 0
    
    # Fill NaN values
    result_df['oz_fbo_stock'] = result_df['oz_fbo_stock'].fillna(0).astype(int)
    result_df['oz_orders_14'] = result_df['oz_orders_14'].fillna(0).astype(int)
    
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
    
    Args:
        df: DataFrame with all WB-Ozon SKU mappings
        min_stock: Minimum required stock level
        min_candidates: Minimum number of candidates required per group (groups with fewer are excluded)
        max_candidates: Maximum number of candidates to select PER GROUP
    
    Returns:
        DataFrame with selected candidates from all groups that meet minimum requirements
    """
    if df.empty:
        return pd.DataFrame()
    
    all_selected = []
    
    # Process each group separately
    for group_num in df['group_num'].unique():
        group_df = df[df['group_num'] == group_num].copy()
        
        # Filter by minimum stock within this group
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

# Search button
if st.button("🔍 Найти связанные артикулы", type="primary"):
    if not wb_skus_input.strip():
        st.warning("Пожалуйста, введите артикулы WB для поиска.")
    else:
        wb_sku_list = wb_skus_input.strip().split()
        
        with st.spinner("Поиск связанных артикулов и загрузка данных..."):
            results_df = get_linked_ozon_skus_with_details(conn, wb_sku_list)
            st.session_state.rk_search_results = results_df

# Display results if available
if not st.session_state.rk_search_results.empty:
    results_df = st.session_state.rk_search_results
    
    st.subheader("📊 Найденные связанные артикулы")
    
    # Summary statistics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Групп WB SKU", results_df['group_num'].nunique())
    with col2:
        st.metric("Всего Ozon SKU", len(results_df))
    with col3:
        st.metric("Общий остаток", results_df['oz_fbo_stock'].sum())
    with col4:
        st.metric("Общие заказы за 14 дней", results_df['oz_orders_14'].sum())
    
    # Display full results table
    st.markdown("##### Все найденные артикулы:")
    display_df = results_df.copy()
    
    # Calculate aggregated values per WB SKU group
    group_aggregates = results_df.groupby('wb_sku').agg({
        'oz_fbo_stock': 'sum',
        'oz_orders_14': 'sum'
    }).rename(columns={
        'oz_fbo_stock': 'total_stock_per_wb_sku',
        'oz_orders_14': 'total_orders_per_wb_sku'
    })
    
    # Merge aggregated values back to main dataframe
    display_df = display_df.merge(group_aggregates, left_on='wb_sku', right_index=True, how='left')
    
    # Rename columns for display
    display_df = display_df.rename(columns={
        'group_num': 'Группа',
        'wb_sku': 'WB SKU', 
        'oz_sku': 'Ozon SKU',
        'oz_fbo_stock': 'Остаток',
        'oz_orders_14': 'Заказов за 14 дней',
        'total_stock_per_wb_sku': 'Остаток общий',
        'total_orders_per_wb_sku': 'Заказы общие'
    })
    
    # Reorder columns to show aggregated data after individual data
    display_df = display_df[['Группа', 'WB SKU', 'Ozon SKU', 'Остаток', 'Заказов за 14 дней', 'Остаток общий', 'Заказы общие']]
    
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    st.markdown("---")
    
    # Advertising selection section
    st.subheader("🎯 Отбор для рекламы")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.info(f"**Критерии отбора:**\n"
                f"• Минимальный остаток: {min_stock_setting} шт\n"
                f"• Минимум кандидатов: {min_candidates_setting} из каждой группы\n"
                f"• Максимум кандидатов: до {max_candidates_setting} из каждой группы\n"
                f"• Сортировка: по убыванию заказов за 14 дней внутри каждой группы\n"
                f"• **Важно:** Группы с количеством подходящих артикулов меньше {min_candidates_setting} будут полностью исключены")
    
    with col2:
        if st.button("🚀 Отобрать кандидатов для рекламы", type="secondary"):
            selected_df = select_advertising_candidates(
                results_df, 
                min_stock=min_stock_setting,
                min_candidates=min_candidates_setting,
                max_candidates=max_candidates_setting
            )
            
            if not selected_df.empty:
                # Calculate statistics by group
                groups_with_candidates = selected_df['group_num'].nunique()
                total_candidates = len(selected_df)
                
                st.success(f"✅ Отобрано {total_candidates} кандидатов из {groups_with_candidates} групп для рекламы")
                
                # Display selected candidates grouped by WB SKU
                st.markdown("##### 🎯 Рекомендованные артикулы для рекламы:")
                
                selected_display_df = selected_df.copy()
                selected_display_df = selected_display_df.rename(columns={
                    'group_num': 'Группа',
                    'wb_sku': 'WB SKU', 
                    'oz_sku': 'Ozon SKU',
                    'oz_fbo_stock': 'Остаток',
                    'oz_orders_14': 'Заказов за 14 дней'
                })
                
                # Add ranking within each group
                selected_display_df['Рейтинг в группе'] = selected_display_df.groupby('Группа').cumcount() + 1
                
                # Reorder columns
                selected_display_df = selected_display_df[['Группа', 'WB SKU', 'Рейтинг в группе', 'Ozon SKU', 'Остаток', 'Заказов за 14 дней']]
                
                # Style the dataframe to highlight by groups
                def highlight_by_group(row):
                    if row['Рейтинг в группе'] == 1:
                        return ['background-color: #e8f5e8'] * len(row)  # Green for #1 in group
                    elif row['Рейтинг в группе'] <= 3:
                        return ['background-color: #f0f8ff'] * len(row)  # Light blue for top 3
                    else:
                        return [''] * len(row)
                
                styled_df = selected_display_df.style.apply(highlight_by_group, axis=1)
                st.dataframe(styled_df, use_container_width=True, hide_index=True)
                
                # Summary by groups
                st.markdown("##### 📈 Сводка по группам:")
                
                # Create group summary
                group_summary = selected_df.groupby(['group_num', 'wb_sku']).agg({
                    'oz_sku': 'count',
                    'oz_fbo_stock': 'sum',
                    'oz_orders_14': 'sum'
                }).rename(columns={
                    'oz_sku': 'Кандидатов',
                    'oz_fbo_stock': 'Общий остаток',
                    'oz_orders_14': 'Общие заказы за 14 дней'
                }).reset_index()
                
                group_summary = group_summary.rename(columns={
                    'group_num': 'Группа',
                    'wb_sku': 'WB SKU'
                })
                
                st.dataframe(group_summary, use_container_width=True, hide_index=True)
                
                # Overall summary for selected candidates
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Всего артикулов", len(selected_df))
                with col2:
                    st.metric("Общий остаток", selected_df['oz_fbo_stock'].sum())
                with col3:
                    st.metric("Общие заказы за 14 дней", selected_df['oz_orders_14'].sum())
                
                # Export option
                if st.button("📋 Копировать Ozon SKU для рекламы"):
                    ozon_skus_list = selected_df['oz_sku'].tolist()
                    ozon_skus_text = ' '.join(ozon_skus_list)
                    st.code(ozon_skus_text, language="text")
                    st.success("Скопируйте Ozon SKU из поля выше для использования в рекламной системе")
                    
                    # Also show breakdown by groups
                    st.markdown("##### 📋 Разбивка по группам:")
                    for group_num in sorted(selected_df['group_num'].unique()):
                        group_data = selected_df[selected_df['group_num'] == group_num]
                        wb_sku = group_data['wb_sku'].iloc[0]
                        group_skus = group_data['oz_sku'].tolist()
                        st.write(f"**Группа {group_num} (WB SKU: {wb_sku}):** {' '.join(group_skus)}")
                    
            else:
                st.warning(f"❌ Не найдено кандидатов, соответствующих критериям:\n"
                          f"- Минимальный остаток: {min_stock_setting} шт\n"
                          f"- Минимум кандидатов: {min_candidates_setting} из каждой группы\n"
                          f"- Максимум кандидатов: до {max_candidates_setting} из каждой группы\n"
                          f"- Возможно, в группах недостаточно артикулов с нужным остатком\n"
                          f"- Попробуйте уменьшить минимальный остаток или минимум кандидатов")

# Instructions section
with st.expander("📖 Инструкция по использованию"):
    st.markdown("""
    ### Как использовать Ozon RK Manager:
    
    1. **Ввод данных**: Введите список WB SKU через пробел в текстовое поле
    
    2. **Поиск связанных артикулов**: Нажмите "Найти связанные артикулы"
       - Система найдет все Ozon SKU, связанные с WB SKU через общие штрихкоды
       - Загрузит данные об остатках и заказах за последние 14 дней
       - Сгруппирует результаты по WB SKU
    
    3. **Анализ таблицы результатов**: В таблице "Все найденные артикулы" отображаются:
       - **Остаток** - индивидуальный остаток конкретного Ozon SKU
       - **Заказов за 14 дней** - индивидуальное количество заказов конкретного Ozon SKU
       - **Остаток общий** - суммарный остаток всех Ozon SKU в пределах одного WB SKU
       - **Заказы общие** - суммарное количество заказов всех Ozon SKU в пределах одного WB SKU
    
    4. **Настройка критериев**: Установите параметры для отбора:
       - **Минимальный остаток**: товары с меньшим остатком не будут рассматриваться
       - **Минимум кандидатов**: минимальное количество подходящих артикулов в группе (если меньше - вся группа исключается)
       - **Максимум кандидатов**: максимальное количество артикулов для отбора из каждой группы
    
    5. **Отбор для рекламы**: Нажмите "Отобрать кандидатов для рекламы"
       - Система сначала проверит, есть ли в каждой группе минимальное количество подходящих артикулов
       - Группы, не соответствующие минимуму, будут полностью исключены
       - Из оставшихся групп отберет топ артикулов по количеству заказов за 14 дней
       - Учтет только артикулы с достаточным остатком
       - Из каждой группы (WB SKU) будет отобрано до N лучших артикулов
    
    6. **Использование результатов**: Скопируйте список Ozon SKU для настройки рекламы
    
    ### Логика работы:
    - **Группировка**: Каждый WB SKU образует отдельную группу
    - **Отбор по группам**: Из каждой группы отбираются лучшие артикулы отдельно
    - **Приоритизация**: Артикулы ранжируются по заказам внутри своей группы
    - **Фильтрация**: Исключаются артикулы с недостаточным остатком в каждой группе
    - **Агрегация**: Общие остатки и заказы помогают оценить потенциал всей группы WB SKU
    """)

# Show help if no results yet
if st.session_state.rk_search_results.empty:
    st.info("👆 Введите WB SKU выше и нажмите 'Найти связанные артикулы' для начала работы")

# Remove the explicit conn.close() when using st.cache_resource for the connection 