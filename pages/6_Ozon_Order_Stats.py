"""
Streamlit page for displaying Ozon order statistics.

This page allows users to:
- Input Ozon SKUs.
- View order statistics for these SKUs over the past 30 days.
- Statistics include a 14-day order sum and daily order counts.
"""
import streamlit as st
from utils.db_connection import connect_db
import pandas as pd
from datetime import datetime, timedelta
# Import necessary functions from db_search_helpers
from utils.db_search_helpers import get_normalized_wb_barcodes, get_ozon_barcodes_and_identifiers

st.set_page_config(page_title="Ozon Order Statistics - Marketplace Analyzer", layout="wide")
st.title("📊 Ozon Order Statistics")
st.markdown("---")

# --- Database Connection ---
@st.cache_resource
def get_connection():
    conn = connect_db()
    if not conn:
        st.error("Database not connected. Please configure the database in Settings.")
        if st.button("Go to Settings", key="db_settings_button_stats"):
            st.switch_page("pages/3_Settings.py")
        st.stop()
    return conn

conn = get_connection()

# --- UI Elements ---
st.subheader("Search Configuration")

col1, col2 = st.columns([1, 2])
with col1:
    search_type_options = {
        "По Ozon SKU": "oz_sku",
        "По Wildberries SKU": "wb_sku"
    }
    search_type_label = st.selectbox(
        "Выберите тип поиска:",
        options=list(search_type_options.keys()),
        index=0
    )
    search_type = search_type_options[search_type_label]

search_values_input = st.text_area( # Renamed from ozon_skus_input for generality
    "Введите SKU для поиска (одно или несколько, разделенных пробелом):",
    height=100,
    help="Например: 12345678 87654321 (для Ozon SKU) или 12345 67890 (для WB SKU)"
)

st.markdown("---")

def fetch_ozon_order_stats(db_conn, oz_skus_list: list[str]) -> pd.DataFrame:
    """
    Fetches and calculates order statistics for given Ozon SKUs.
    - Filters orders for the last 30 days.
    - Excludes orders with status 'Отменён'.
    - Calculates total orders for the last 14 days.
    - Provides daily order counts for the last 29 days.
    """
    if not oz_skus_list:
        return pd.DataFrame()

    # Process SKUs to be strings for the query, ensuring they consist of digits.
    # This aligns better with typical SKU representations and database interactions.
    valid_skus = []
    for s_input in oz_skus_list:
        s_str = str(s_input).strip() # Ensure it's a string and remove whitespace
        if s_str.isdigit(): # Check if the string consists of digits
            valid_skus.append(s_str)
        # Optional: else: st.warning(f"SKU '{s_str}' не является числовым и будет проигнорирован.")

    if not valid_skus:
        st.warning("Не предоставлено корректных Ozon SKU (ожидаются числовые значения, например, '12345678').")
        return pd.DataFrame()

    today = datetime.now()
    thirty_days_ago = today - timedelta(days=30)
    fourteen_days_ago = today - timedelta(days=14)

    query_params = {
        "skus": tuple(valid_skus),
        "start_date_30": thirty_days_ago.strftime('%Y-%m-%d'),
        "start_date_14": fourteen_days_ago.strftime('%Y-%m-%d'),
        "excluded_status": "Отменён"
    }

    # Base query to get relevant orders
    query = f"""
    SELECT
        oz_sku,
        oz_accepted_date,
        order_status,
        1 AS order_count -- Each row is one order for counting purposes
    FROM
        oz_orders
    WHERE
        oz_sku IN ({', '.join(['?'] * len(valid_skus))})
        AND oz_accepted_date >= ?
        AND order_status != ?
    ORDER BY
        oz_sku, oz_accepted_date DESC;
    """
    
    try:
        orders_df = db_conn.execute(query, list(valid_skus) + [query_params["start_date_30"], query_params["excluded_status"]]).fetchdf()
    except Exception as e:
        st.error(f"Ошибка при получении данных о заказах: {e}")
        return pd.DataFrame()

    if orders_df.empty:
        return pd.DataFrame()

    # Ensure oz_accepted_date is datetime
    orders_df['oz_accepted_date'] = pd.to_datetime(orders_df['oz_accepted_date'])

    # --- Prepare the results table ---
    # Create a DataFrame with all SKUs and all dates for the last 29 days
    all_results = []
    
    # Generate date columns for the last 29 days (today-1 to today-29)
    # The column names will be the dates themselves (YYYY-MM-DD) for easier processing
    # The display names in the table header will be formatted later if needed.
    date_columns = [(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(1, 30)]

    for sku in valid_skus:
        sku_data = {"oz_sku": sku}
        
        # Filter orders for this specific SKU
        sku_orders_df = orders_df[orders_df['oz_sku'] == sku].copy() # Use .copy() to avoid SettingWithCopyWarning

        # Calculate 14-day sum
        orders_last_14_days = sku_orders_df[sku_orders_df['oz_accepted_date'] >= pd.to_datetime(query_params["start_date_14"])]
        sku_data["orders_sum_14"] = orders_last_14_days['order_count'].sum()

        # Daily counts for the last 29 days
        # Group by date and sum orders
        sku_orders_df['date_str'] = sku_orders_df['oz_accepted_date'].dt.strftime('%Y-%m-%d')
        daily_counts = sku_orders_df.groupby('date_str')['order_count'].sum()

        for date_col_str in date_columns:
            sku_data[date_col_str] = daily_counts.get(date_col_str, 0)
            
        all_results.append(sku_data)

    if not all_results:
        return pd.DataFrame()

    results_df = pd.DataFrame(all_results)
    
    # Reorder columns: oz_sku, orders_sum_14, then date columns in descending order (today-1, today-2, ...)
    # The date_columns list is already in today-1, today-2 order.
    final_columns_ordered = ["oz_sku", "orders_sum_14"] + date_columns
    results_df = results_df[final_columns_ordered]
    
    # Rename oz_sku to string for consistent merging later if used with WB results
    results_df['oz_sku'] = results_df['oz_sku'].astype(str)

    return results_df

def get_linked_ozon_skus_for_wb_sku(db_conn, wb_sku_list: list[str]) -> dict[str, list[str]]:
    """
    For a list of WB SKUs, finds all linked Ozon SKUs via common barcodes.
    Returns a dictionary mapping each WB SKU to a list of its linked Ozon SKUs.
    """
    if not wb_sku_list:
        return {}

    # Ensure WB SKUs are strings for helper functions
    wb_sku_list_str = [str(sku) for sku in wb_sku_list]

    wb_barcodes_df = get_normalized_wb_barcodes(db_conn, wb_skus=wb_sku_list_str)
    if wb_barcodes_df.empty:
        # st.info("Не найдены штрихкоды для указанных WB SKU.") # Can be noisy
        return {}

    # We need all Ozon barcodes to find matches
    oz_barcodes_ids_df = get_ozon_barcodes_and_identifiers(db_conn)
    if oz_barcodes_ids_df.empty:
        # st.info("Не удалось загрузить штрихкоды Ozon для сопоставления.")
        return {}

    # Rename columns for clarity before merge
    wb_barcodes_df = wb_barcodes_df.rename(columns={'individual_barcode_wb': 'barcode'})
    oz_barcodes_ids_df = oz_barcodes_ids_df.rename(columns={'oz_barcode': 'barcode'})
    
    # Ensure barcode columns are of the same type (string) to avoid merge issues
    wb_barcodes_df['barcode'] = wb_barcodes_df['barcode'].astype(str).str.strip()
    oz_barcodes_ids_df['barcode'] = oz_barcodes_ids_df['barcode'].astype(str).str.strip()
    
    # Keep only relevant columns and drop duplicates before merge
    wb_barcodes_df = wb_barcodes_df[['wb_sku', 'barcode']].drop_duplicates()
    oz_barcodes_ids_df = oz_barcodes_ids_df[['oz_sku', 'barcode']].drop_duplicates()
    
    # Filter out empty barcodes
    wb_barcodes_df = wb_barcodes_df[wb_barcodes_df['barcode'] != '']
    oz_barcodes_ids_df = oz_barcodes_ids_df[oz_barcodes_ids_df['barcode'] != '']

    # Merge to find common barcodes
    # We expect wb_sku to be string, and oz_sku from oz_barcodes_ids_df is likely int, convert for consistency
    oz_barcodes_ids_df['oz_sku'] = oz_barcodes_ids_df['oz_sku'].astype(str)
    wb_barcodes_df['wb_sku'] = wb_barcodes_df['wb_sku'].astype(str)


    merged_df = pd.merge(wb_barcodes_df, oz_barcodes_ids_df, on='barcode', how='inner')

    if merged_df.empty:
        # st.info("Не найдено совпадений Ozon SKU для указанных WB SKU по штрихкодам.")
        return {}

    # Group by wb_sku and collect all unique linked oz_sku
    linked_skus_map = merged_df.groupby('wb_sku')['oz_sku'].apply(lambda x: list(set(x))).to_dict()
    return linked_skus_map


if st.button("🔍 Получить статистику", type="primary"): # Changed button label
    if not search_values_input.strip():
        st.warning("Пожалуйста, введите SKU.")
    else:
        search_values_list = search_values_input.strip().split()
        
        st.markdown("### Результаты Статистики")

        if search_type == "oz_sku":
            with st.spinner("Загрузка статистики по Ozon SKU..."):
                stats_df = fetch_ozon_order_stats(conn, search_values_list)
            
            if not stats_df.empty:
                st.success(f"Статистика сформирована для {len(stats_df)} Ozon SKU.")
                # Prepare for display
                rename_map = {"oz_sku": "Ozon SKU", "orders_sum_14": "Заказов за 14 дней"}
                current_date = datetime.now()
                for i in range(1, 30):
                    date_col = (current_date - timedelta(days=i)).strftime('%Y-%m-%d')
                    if date_col in stats_df.columns:
                        rename_map[date_col] = f"{(current_date - timedelta(days=i)).strftime('%d.%m')}"
                
                display_df = stats_df.rename(columns=rename_map)
                cols_to_display = ["Ozon SKU", "Заказов за 14 дней"] + [
                    f"{(current_date - timedelta(days=i)).strftime('%d.%m')}" for i in range(1, 30)
                    if f"{(current_date - timedelta(days=i)).strftime('%d.%m')}" in display_df.columns
                ]
                cols_to_display = [col for col in cols_to_display if col in display_df.columns]
                st.dataframe(display_df[cols_to_display], use_container_width=True, hide_index=True)
            else:
                st.info("По вашему запросу Ozon SKU нет данных или не удалось сформировать статистику.")

        elif search_type == "wb_sku":
            with st.spinner("Поиск связанных Ozon SKU и загрузка статистики..."):
                linked_oz_skus_map = get_linked_ozon_skus_for_wb_sku(conn, search_values_list)

            if not linked_oz_skus_map:
                st.info("Не найдено связанных Ozon SKU для указанных WB SKU, или нет данных для статистики.")
            else:
                st.success(f"Найдены связанные Ozon SKU для {len(linked_oz_skus_map)} WB SKU.")
                
                all_wb_stats_display_list = []

                for wb_sku, oz_skus in linked_oz_skus_map.items():
                    if not oz_skus:
                        # Add a row for WB SKU with no linked Ozon SKUs or no orders
                        wb_summary_row = {"WB SKU": wb_sku, "Кол-во Ozon SKU": 0, "Заказов за 14 дней (∑)": 0}
                        current_date = datetime.now()
                        for i in range(1,30): # Add day columns with 0
                            day_col_name = f"{(current_date - timedelta(days=i)).strftime('%d.%m')}"
                            wb_summary_row[day_col_name] = 0
                        all_wb_stats_display_list.append(wb_summary_row)
                        # st.write(f"Для WB SKU {wb_sku} не найдено связанных Ozon SKU с заказами.")
                        continue

                    oz_sku_stats_df = fetch_ozon_order_stats(conn, oz_skus)

                    if oz_sku_stats_df.empty:
                        wb_summary_row = {"WB SKU": wb_sku, "Кол-во Ozon SKU": len(oz_skus), "Заказов за 14 дней (∑)": 0}
                        current_date = datetime.now()
                        for i in range(1,30): # Add day columns with 0
                            day_col_name = f"{(current_date - timedelta(days=i)).strftime('%d.%m')}"
                            wb_summary_row[day_col_name] = 0
                        all_wb_stats_display_list.append(wb_summary_row)
                        # st.write(f"Для Ozon SKU ({', '.join(oz_skus)}), связанных с WB SKU {wb_sku}, нет данных о заказах.")
                        continue
                    
                    # Aggregate stats for the WB SKU
                    total_oz_skus_found = len(oz_sku_stats_df['oz_sku'].unique()) # Count based on actual stats returned
                    sum_14_days_total = oz_sku_stats_df["orders_sum_14"].sum()
                    
                    # Sum daily counts
                    # Date columns in oz_sku_stats_df are YYYY-MM-DD
                    date_columns_raw = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(1, 30)]
                    
                    wb_summary_row = {
                        "WB SKU": wb_sku,
                        "Кол-во Ozon SKU": total_oz_skus_found,
                        "Заказов за 14 дней (∑)": sum_14_days_total
                    }
                    
                    current_date_for_header = datetime.now()
                    for date_col_raw in date_columns_raw:
                        day_col_display_name = f"{(datetime.strptime(date_col_raw, '%Y-%m-%d')).strftime('%d.%m')} (Сегодня-{datetime.now().day - datetime.strptime(date_col_raw, '%Y-%m-%d').day})"
                        # Calculate day difference more reliably
                        day_diff = (current_date_for_header.date() - datetime.strptime(date_col_raw, '%Y-%m-%d').date()).days
                        day_col_display_name = f"{(datetime.strptime(date_col_raw, '%Y-%m-%d')).strftime('%d.%m')}"
                        
                        if date_col_raw in oz_sku_stats_df.columns:
                            wb_summary_row[day_col_display_name] = oz_sku_stats_df[date_col_raw].sum()
                        else: # Should not happen if fetch_ozon_order_stats always returns all date columns
                            wb_summary_row[day_col_display_name] = 0
                    
                    all_wb_stats_display_list.append(wb_summary_row)

                    # Display individual Ozon SKU stats within an expander
                    with st.expander(f"Детализация по WB SKU: {wb_sku} (найдено {total_oz_skus_found} Ozon SKU)"):
                        if not oz_sku_stats_df.empty:
                            rename_map_oz = {"oz_sku": "Ozon SKU", "orders_sum_14": "Заказов за 14 дней"}
                            current_date = datetime.now()
                            for i in range(1, 30):
                                date_col = (current_date - timedelta(days=i)).strftime('%Y-%m-%d')
                                if date_col in oz_sku_stats_df.columns:
                                    day_diff = (current_date.date() - (current_date - timedelta(days=i)).date()).days
                                    rename_map_oz[date_col] = f"{(current_date - timedelta(days=i)).strftime('%d.%m')}"
                            
                            oz_display_df = oz_sku_stats_df.rename(columns=rename_map_oz)
                            
                            cols_to_display_oz = ["Ozon SKU", "Заказов за 14 дней"] + [
                                rename_map_oz[col] for col in date_columns_raw if rename_map_oz.get(col) in oz_display_df.columns
                            ]
                            cols_to_display_oz = [col for col in cols_to_display_oz if col in oz_display_df.columns]


                            st.dataframe(oz_display_df[cols_to_display_oz], use_container_width=True, hide_index=True)
                        else:
                            st.write("Нет данных о заказах для связанных Ozon SKU.")
                
                if all_wb_stats_display_list:
                    summary_wb_df = pd.DataFrame(all_wb_stats_display_list)
                    # Define display order for summary_wb_df columns
                    summary_cols_ordered = ["WB SKU", "Кол-во Ozon SKU", "Заказов за 14 дней (∑)"]
                    current_date = datetime.now()
                    for i in range(1,30):
                        day_diff = i
                        day_col_name = f"{(current_date - timedelta(days=i)).strftime('%d.%m')}"
                        if day_col_name in summary_wb_df.columns: # Check if column exists
                             summary_cols_ordered.append(day_col_name)
                    
                    # Filter summary_cols_ordered to only include columns present in summary_wb_df
                    summary_cols_ordered = [col for col in summary_cols_ordered if col in summary_wb_df.columns]


                    st.markdown("#### Сводная статистика по WB SKU")
                    st.dataframe(summary_wb_df[summary_cols_ordered], use_container_width=True, hide_index=True)
                else: # This case might be redundant given the previous checks.
                    st.info("Не удалось сформировать сводную статистику по WB SKU.")

# Remove the explicit conn.close() when using st.cache_resource for the connection
# if conn:
#     conn.close() 