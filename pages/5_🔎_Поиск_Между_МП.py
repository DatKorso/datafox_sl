"""
Streamlit page for cross-marketplace product search.

This page allows users to:
- Select a search criterion (WB SKU, Ozon SKU, Ozon Vendor Code, Barcode).
- Input search values (single or multiple).
- Select which information fields to display for matched products.
- View results in a table.

NEW FEATURES:
- Added support for oz_category_products table fields:
  * merge_on_card - Объединить на карточке
  * color_name - Название цвета  
  * manufacturer_size - Размер производителя
- These fields are linked via oz_vendor_code and are optional (not in default selection).
"""
import streamlit as st
from utils.db_connection import connect_db
from utils.cross_marketplace_linker import CrossMarketplaceLinker
import pandas as pd

st.set_page_config(page_title="Cross-Marketplace Search - Marketplace Analyzer", layout="wide")
st.title("🔄 Cross-Marketplace Product Search")
st.markdown("---")

# --- Database Connection ---
conn = connect_db()
if not conn:
    st.error("Database not connected. Please configure the database in Settings.")
    if st.button("Go to Settings"):
        st.switch_page("pages/3_Settings.py")
    st.stop()

# --- UI Elements ---
st.subheader("Search Configuration")

col1, col2 = st.columns([1, 2])

with col1:
    search_criterion_options = {
        "Артикул WB": "wb_sku",
        "Артикул Ozon": "oz_sku",
        "Код поставщика Ozon": "oz_vendor_code",
        "Штрихкод": "barcode"
    }
    search_criterion_label = st.selectbox(
        "По какому критерию производить поиск?",
        options=list(search_criterion_options.keys()),
        index=0
    )
    search_criterion = search_criterion_options[search_criterion_label]

search_values_input = st.text_area(
    "Введите значения для поиска (одно или несколько, разделенных пробелом):",
    height=100,
    help="Например: 12345 67890"
)

st.markdown("---")
st.subheader("Information to Display")

st.info("💡 **Новинка**: Теперь доступны дополнительные поля из детальных характеристик товаров Ozon (таблица oz_category_products), включая объединение на карточке, название цвета и размер производителя.\n\n🎯 **Актуальный штрихкод**: Показывает 'Да' для первого штрихкода в списке WB (разделенных ';'), который дал совпадение с товаром Ozon. Если первый штрихкод не совпал, но совпал второй - он станет актуальным.")

# Define all possible fields that can be displayed
# Keys are user-friendly labels, values are (table_alias, column_name_in_db) or special identifiers
# This will be used later to construct the SELECT part of the SQL query dynamically
# and to name columns in the final DataFrame.
# 'common_barcode' is a special case that will be derived from the join.

# OZON Fields
ozon_fields_options = {
    "Ozon: Артикул (oz_sku)": ("oz_products", "oz_sku"),
    "Ozon: Код поставщика (oz_vendor_code)": ("oz_products", "oz_vendor_code"),
    "Ozon: Product ID (oz_product_id)": ("oz_products", "oz_product_id"),
    "Ozon: Бренд (oz_brand)": ("oz_products", "oz_brand"),
    "Ozon: Статус (oz_product_status)": ("oz_products", "oz_product_status"),
    "Ozon: Цена (oz_actual_price)": ("oz_products", "oz_actual_price"),
    "Ozon: Остаток (oz_fbo_stock)": ("oz_products", "oz_fbo_stock"),
    "Ozon: Штрихкод (oz_barcode)": ("oz_barcodes", "oz_barcode"), # From oz_barcodes table
    "Ozon: Объединить на карточке (merge_on_card)": ("oz_category_products", "merge_on_card"), # From oz_category_products table
    "Ozon: Название цвета (color_name)": ("oz_category_products", "color_name"), # From oz_category_products table
    "Ozon: Размер производителя (manufacturer_size)": ("oz_category_products", "manufacturer_size") # From oz_category_products table
}

# Wildberries Fields
wb_fields_options = {
    "WB: Артикул (wb_sku)": ("wb_products", "wb_sku"),
    "WB: Бренд (wb_brand)": ("wb_products", "wb_brand"),
    "WB: Категория (wb_category)": ("wb_products", "wb_category"),
    "WB: Штрихкоды (wb_barcodes)": ("wb_products", "wb_barcodes"), # Original string from wb_products
    "WB: Цена (wb_full_price)": ("wb_prices", "wb_full_price"), # From wb_prices table
}

# Common/Derived Fields
common_fields_options = {
    "Общий штрихкод (по которому найдено совпадение)": "common_matched_barcode", # Special identifier
    "Актуальный штрихкод": "is_primary_barcode" # NEW: Special identifier for primary barcode status
}

all_display_options = list(common_fields_options.keys()) + list(ozon_fields_options.keys()) + list(wb_fields_options.keys())

# Default selections:
default_selections = [
    "Общий штрихкод (по которому найдено совпадение)",
    "Актуальный штрихкод",  # NEW: Add primary barcode status to default
    "Ozon: Артикул (oz_sku)",
    "Ozon: Код поставщика (oz_vendor_code)",
    "Ozon: Остаток (oz_fbo_stock)",
    "WB: Артикул (wb_sku)",
]


selected_display_fields_labels = st.multiselect(
    "Выберите, какую информацию показывать в результатах:",
    options=all_display_options,
    default=[opt for opt in default_selections if opt in all_display_options],
    help="Выберите поля, которые вы хотите видеть в итоговой таблице."
)

st.markdown("---")

if st.button("🚀 Найти совпадающие товары", type="primary"):
    if not search_values_input.strip():
        st.warning("Пожалуйста, введите значения для поиска.")
    elif not selected_display_fields_labels:
        st.warning("Пожалуйста, выберите хотя бы одно поле для отображения в результатах.")
    else:
        search_values = search_values_input.strip().split()
        
        st.info(f"Выполняется поиск по критерию: '{search_criterion_label}' для значений: {search_values}")
        # st.write(f"Выбранные поля для отображения: {', '.join(selected_display_fields_labels)}")
        
        # Prepare selected_fields_map for the db_utils function
        selected_fields_map_for_query = {}
        for label in selected_display_fields_labels:
            if label in common_fields_options:
                selected_fields_map_for_query[label] = common_fields_options[label]
            elif label in ozon_fields_options:
                selected_fields_map_for_query[label] = ozon_fields_options[label]
            elif label in wb_fields_options:
                selected_fields_map_for_query[label] = wb_fields_options[label]

        with st.spinner("Идет поиск совпадений... Это может занять некоторое время..."):
            # Используем новый централизованный модуль связывания
            linker = CrossMarketplaceLinker(conn)
            results_df = linker.find_marketplace_matches(
                search_criterion=search_criterion, # 'wb_sku', 'oz_sku', etc.
                search_values=search_values,
                selected_fields_map=selected_fields_map_for_query
            )

        st.markdown("### Результаты Поиска")
        if not results_df.empty:
            st.success(f"Найдено {len(results_df)} совпадений.")
            # Reorder columns to have "Search_Value" first, then others as selected by user
            # The find_cross_marketplace_matches function already aliases the search criterion column to "Search_Value"
            # and other columns to their UI labels.
            
            # Ensure the "Search_Value" column (if it exists) is first.
            cols = list(results_df.columns)
            if "Search_Value" in cols:
                cols.insert(0, cols.pop(cols.index("Search_Value")))
                # Ensure other selected columns follow the order from multiselect if possible,
                # though dict ordering for selected_fields_map_for_query might not preserve it perfectly.
                # For now, just ensuring Search_Value is first and others follow is good.
                # A more robust reordering would map selected_display_fields_labels to the actual column names in results_df
                ordered_cols = ["Search_Value"]
                for label in selected_display_fields_labels: # These are the keys from selected_fields_map_for_query
                    if label in results_df.columns and label != "Search_Value":
                        ordered_cols.append(label)
                # Add any remaining columns from results_df not in ordered_cols (e.g. if a default field was added by search fn)
                for col in results_df.columns:
                    if col not in ordered_cols:
                        ordered_cols.append(col)
                results_df = results_df[ordered_cols]
            
            st.dataframe(results_df, use_container_width=True, hide_index=True)
        else:
            st.info("По вашему запросу ничего не найдено, или произошла ошибка при получении данных.")

if conn:
    conn.close() 