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
- Added session state support to preserve data when switching between display modes
"""
import streamlit as st
from utils.db_connection import connect_db
from utils.cross_marketplace_linker import CrossMarketplaceLinker
from utils.wb_photo_service import get_wb_photo_url
import pandas as pd

st.set_page_config(page_title="Cross-Marketplace Search - Marketplace Analyzer", layout="wide")
st.title("🔄 Cross-Marketplace Product Search")
st.markdown("---")

# 🔄 Initialize session state for preserving data between interactions
if 'search_executed' not in st.session_state:
    st.session_state.search_executed = False
if 'search_results' not in st.session_state:
    st.session_state.search_results = None
if 'search_info' not in st.session_state:
    st.session_state.search_info = None

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
    
    # Use session state to remember selection
    default_index = 0
    if 'last_search_criterion' in st.session_state:
        for i, key in enumerate(search_criterion_options.keys()):
            if search_criterion_options[key] == st.session_state.last_search_criterion:
                default_index = i
                break
    
    search_criterion_label = st.selectbox(
        "По какому критерию производить поиск?",
        options=list(search_criterion_options.keys()),
        index=default_index,
        key="search_criterion_select"
    )
    search_criterion = search_criterion_options[search_criterion_label]

# Use session state to remember search input
search_values_input = st.text_area(
    "Введите значения для поиска (одно или несколько, разделенных пробелом):",
    height=100,
    help="Например: 12345 67890",
    value=st.session_state.get('last_search_values', ''),
    key="search_values_textarea"
)

st.markdown("---")

# Define all possible fields that can be displayed
# Keys are user-friendly labels, values are (table_alias, column_name_in_db) or special identifiers
# This will be used later to construct the SELECT part of the SQL query dynamically
# and to name columns in the final DataFrame.
# 'common_barcode' is a special case that will be derived from the join.

# OZON Fields
ozon_fields_options = {
    "Oz: SKU": ("oz_products", "oz_sku"),
    "Oz: Артикул поставщика": ("oz_products", "oz_vendor_code"),
    "Oz: Product ID (oz_product_id)": ("oz_products", "oz_product_id"),
    "Oz: Бренд (oz_brand)": ("oz_products", "oz_brand"),
    "Oz: Статус (oz_product_status)": ("oz_products", "oz_product_status"),
    "Oz: Цена (oz_actual_price)": ("oz_products", "oz_actual_price"),
    "Oz: Остаток (oz_fbo_stock)": ("oz_products", "oz_fbo_stock"),
    "Oz: Штрихкод (oz_barcode)": ("oz_barcodes", "oz_barcode"), # From oz_barcodes table
    "Oz: Объединить на карточке (merge_on_card)": ("oz_category_products", "merge_on_card"), # From oz_category_products table
    "Oz: Название цвета (color_name)": ("oz_category_products", "color_name"), # From oz_category_products table
    "Oz: Размер производителя (manufacturer_size)": ("oz_category_products", "manufacturer_size"), # From oz_category_products table
    "Oz: Изображение (main_photo_url)": ("oz_category_products", "main_photo_url") # From oz_category_products table
}

# Wildberries Fields
wb_fields_options = {
    "WB: SKU": ("wb_products", "wb_sku"),
    "WB: Бренд (wb_brand)": ("wb_products", "wb_brand"),
    "WB: Категория (wb_category)": ("wb_products", "wb_category"),
    "WB: Штрихкоды (wb_barcodes)": ("wb_products", "wb_barcodes"), # Original string from wb_products
    "WB: Цена (wb_full_price)": ("wb_prices", "wb_full_price"), # From wb_prices table
}

# Common/Derived Fields
common_fields_options = {
    "Общий ШК": "common_matched_barcode", # Special identifier
    "Актуальный штрихкод": "is_primary_barcode" # NEW: Special identifier for primary barcode status
}

all_display_options = list(common_fields_options.keys()) + list(ozon_fields_options.keys()) + list(wb_fields_options.keys())

# Default selections:
default_selections = [
    "Общий ШК",
    "Актуальный штрихкод",  # NEW: Add primary barcode status to default
    "Oz: SKU",
    "Oz: Артикул поставщика",
    "Oz: Остаток",
    "WB: SKU",
]

# Use session state to remember field selections
default_selected = st.session_state.get('last_selected_fields', [opt for opt in default_selections if opt in all_display_options])

selected_display_fields_labels = st.multiselect(
    "Выберите, какую информацию показывать в результатах:",
    options=all_display_options,
    default=default_selected,
    help="Выберите поля, которые вы хотите видеть в итоговой таблице.",
    key="selected_fields_multiselect"
)

st.markdown("---")

# Add buttons for search and clearing results
col_btn1, col_btn2 = st.columns([2, 1])

with col_btn1:
    search_button_clicked = st.button("🚀 Найти совпадающие товары", type="primary")

with col_btn2:
    if st.session_state.search_executed:
        clear_button_clicked = st.button("🗑️ Очистить результаты", help="Очистить результаты поиска и форму")
    else:
        clear_button_clicked = False

# Handle search button click
if search_button_clicked:
    if not search_values_input.strip():
        st.warning("Пожалуйста, введите значения для поиска.")
    elif not selected_display_fields_labels:
        st.warning("Пожалуйста, выберите хотя бы одно поле для отображения в результатах.")
    else:
        search_values = search_values_input.strip().split()
        
        # Save current form state to session state
        st.session_state.last_search_values = search_values_input
        st.session_state.last_search_criterion = search_criterion
        st.session_state.last_selected_fields = selected_display_fields_labels
        
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

        # Save results and info to session state
        st.session_state.search_results = results_df
        st.session_state.search_info = {
            'criterion_label': search_criterion_label,
            'values': search_values,
            'fields_map': selected_fields_map_for_query,
            'selected_fields': selected_display_fields_labels
        }
        st.session_state.search_executed = True
        
        # Show immediate feedback
        st.info(f"Выполнен поиск по критерию: '{search_criterion_label}' для значений: {search_values}")

# Handle clear button click
if clear_button_clicked:
    # Clear all session state
    st.session_state.search_executed = False
    st.session_state.search_results = None
    st.session_state.search_info = None
    st.session_state.last_search_values = ''
    if 'last_search_criterion' in st.session_state:
        del st.session_state.last_search_criterion
    if 'last_selected_fields' in st.session_state:
        del st.session_state.last_selected_fields
    st.success("Результаты поиска и форма очищены!")
    st.rerun()

# Display results if they exist in session state
if st.session_state.search_executed and st.session_state.search_results is not None:
    results_df = st.session_state.search_results
    search_info = st.session_state.search_info
    
    st.markdown("### Результаты Поиска")
    
    if not results_df.empty:
        st.success(f"Найдено {len(results_df)} совпадений.")
        
        # Display search criteria info
        with st.expander("ℹ️ Информация о поиске", expanded=False):
            st.info(f"""
            **Критерий поиска**: {search_info['criterion_label']}  
            **Значения для поиска**: {', '.join(map(str, search_info['values']))}  
            **Выбранные поля**: {', '.join(search_info['selected_fields'])}
            """)
        
        # Function to add product photos (moved here from inside button block)
        def add_product_photos(df):
            """Добавляет столбец с фотографиями товаров WB в начало таблицы."""
            # Ищем столбец с WB SKU - обновленная логика поиска
            wb_sku_column = None
            for col in df.columns:
                col_str = str(col).lower()
                # Расширенный поиск колонки с WB SKU
                if ('wb_sku' in col_str or 
                    'артикул wb' in col_str or 
                    'wb: sku' in col_str or
                    col == 'WB: SKU'):
                    wb_sku_column = col
                    break
            
            if wb_sku_column and wb_sku_column in df.columns:
                # Создаем столбец с URL для фотографий
                def get_photo_url(wb_sku):
                    if pd.isna(wb_sku) or str(wb_sku).strip() == '':
                        return None
                    
                    try:
                        photo_url = get_wb_photo_url(str(wb_sku))
                        return photo_url if photo_url else None
                    except Exception as e:
                        return None
                
                # Добавляем столбец с URL фотографий
                df_with_photos = df.copy()
                df_with_photos.insert(0, '🖼️ Фото WB', df_with_photos[wb_sku_column].apply(get_photo_url))
                return df_with_photos
            
            return df
        
        # Reorder columns to have "Search_Value" first, then others as selected by user
        cols = list(results_df.columns)
        if "Search_Value" in cols:
            cols.insert(0, cols.pop(cols.index("Search_Value")))
            ordered_cols = ["Search_Value"]
            for label in search_info['selected_fields']:
                if label in results_df.columns and label != "Search_Value":
                    ordered_cols.append(label)
            # Add any remaining columns from results_df not in ordered_cols
            for col in results_df.columns:
                if col not in ordered_cols:
                    ordered_cols.append(col)
            results_df = results_df[ordered_cols]
        
        # 🎯 ПРАВИЛЬНОЕ РЕШЕНИЕ: Использование ImageColumn для отображения изображений в интерактивной таблице
        results_with_photos_df = add_product_photos(results_df)
        
        # Настройка конфигурации колонок для правильного отображения изображений
        column_config = {}
        
        # Если есть колонка с фотографиями, настраиваем её как ImageColumn
        if '🖼️ Фото WB' in results_with_photos_df.columns:
            column_config['🖼️ Фото WB'] = st.column_config.ImageColumn(
                "🖼️ Фото WB",
                help="Фотография товара WB",
                width="small"
            )
        
        # Отображаем интерактивную таблицу с изображениями в ячейках
        st.markdown("**📊 Интерактивная таблица с изображениями в ячейках:**")
        st.dataframe(
            results_with_photos_df, 
            use_container_width=True, 
            hide_index=True,
            column_config=column_config,
            height=400  # Фиксированная высота для лучшего просмотра
        )
                
    else:
        st.info("По вашему запросу ничего не найдено, или произошла ошибка при получении данных.")

# Display helpful message when no search has been executed yet
elif not st.session_state.search_executed:
    st.markdown("### 🔍 Начните поиск")
    st.info("👆 Введите критерии поиска выше и нажмите кнопку **'🚀 Найти совпадающие товары'** для начала.")
    
    # Show some helpful tips
    with st.expander("💡 Полезные советы", expanded=False):
        st.markdown("""
        **Как использовать поиск:**
        
        1. **Выберите критерий поиска** - по какому полю искать совпадения
        2. **Введите значения** - можно несколько значений через пробел
        3. **Выберите поля для отображения** - какую информацию показать в результатах
        4. **Нажмите кнопку поиска** - результаты сохранятся и будут доступны при переключении режимов отображения
        
        **Преимущества новой системы:**
        - ✅ Результаты сохраняются при переключении между режимами отображения
        - ✅ Можно изменять способ отображения без повторного поиска
        - ✅ Кнопка "Очистить результаты" для сброса всех данных
        """)

if conn:
    conn.close() 