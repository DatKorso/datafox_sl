"""
Streamlit page for analyzing card problems in Ozon products.

This page allows users to:
- View and analyze the 'error' field from oz_category_products table
- Parse error messages that are separated by ";" symbol
- Filter products by specific error types
- Get statistics and insights about different error types
- Export filtered data

The error field contains a list of issues separated by ";". For example:
"Неверно заполнен аттрибут цвет; фото некорректного разрешения"

This page dynamically parses existing error values and provides filtering capabilities.
"""

import streamlit as st
import pandas as pd
import numpy as np
from collections import Counter
from utils.config_utils import get_db_path
from utils.db_connection import connect_db
import re

st.set_page_config(page_title="Анализ проблем карточек Ozon", layout="wide")
st.title("🚨 Анализ проблем карточек товаров Ozon")

# Database connection setup
db_path = get_db_path()

if not db_path:
    st.warning("Путь к базе данных не настроен. Пожалуйста, настройте его на странице 'Настройки'.")
    if st.button("Перейти в Настройки"):
        st.switch_page("pages/3_Settings.py")
    st.stop()

db_connection = connect_db(db_path)

if not db_connection:
    st.error(f"Не удалось подключиться к базе данных: {db_path}.")
    if st.button("Перейти в Настройки"):
        st.switch_page("pages/3_Settings.py")
    st.stop()

# Check if oz_category_products table exists
try:
    table_check_query = """
        SELECT COUNT(*) as table_exists 
        FROM information_schema.tables 
        WHERE table_name = 'oz_category_products' AND table_schema = 'main'
    """
    table_exists = db_connection.execute(table_check_query).fetchone()[0] > 0
    
    if not table_exists:
        st.error("Таблица `oz_category_products` не найдена в базе данных.")
        st.info("Пожалуйста, импортируйте данные Ozon на странице 'Импорт отчетов'.")
        if st.button("Перейти к Импорту"):
            st.switch_page("pages/2_Import_Reports.py")
        st.stop()
        
except Exception as e:
    st.error(f"Ошибка при проверке таблицы: {e}")
    st.stop()

st.success(f"Соединение с базой данных '{db_path}' установлено.")

# Cache function for loading data
@st.cache_data
def load_error_data():
    """Load products with error information from the database."""
    query = """
        SELECT 
            oz_vendor_code,
            product_name,
            oz_brand,
            color,
            russian_size,
            error,
            warning,
            oz_actual_price,
            oz_sku
        FROM oz_category_products 
        WHERE error IS NOT NULL AND error != '' AND error != 'NULL'
        ORDER BY oz_vendor_code
    """
    try:
        df = db_connection.execute(query).fetchdf()
        return df
    except Exception as e:
        st.error(f"Ошибка при загрузке данных: {e}")
        return pd.DataFrame()

@st.cache_data
def parse_error_types(df):
    """Parse and extract unique error types from the error field."""
    if df.empty:
        return []
    
    all_errors = []
    for error_text in df['error'].dropna():
        if pd.isna(error_text) or error_text in ['', 'NULL']:
            continue
        # Split by semicolon and clean up
        errors = [error.strip() for error in str(error_text).split(';') if error.strip()]
        all_errors.extend(errors)
    
    # Count occurrences and return sorted by frequency
    error_counts = Counter(all_errors)
    return sorted(error_counts.items(), key=lambda x: x[1], reverse=True)

# Load data
with st.spinner("Загрузка данных о проблемах карточек..."):
    error_df = load_error_data()

if error_df.empty:
    st.info("В таблице `oz_category_products` не найдено товаров с ошибками.")
    st.info("Возможные причины:")
    st.write("- Поле 'error' пустое для всех записей")
    st.write("- Данные еще не импортированы")
    st.write("- Все карточки не имеют проблем")
    st.stop()

# Parse error types
error_types = parse_error_types(error_df)

st.subheader("📊 Общая статистика")

# Display key metrics
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Товаров с ошибками", len(error_df))
with col2:
    st.metric("Уникальных типов ошибок", len(error_types))
with col3:
    total_products_query = "SELECT COUNT(*) FROM oz_category_products"
    total_products = db_connection.execute(total_products_query).fetchone()[0]
    error_percentage = (len(error_df) / total_products * 100) if total_products > 0 else 0
    st.metric("% товаров с ошибками", f"{error_percentage:.1f}%")
with col4:
    brands_with_errors = error_df['oz_brand'].nunique()
    st.metric("Брендов с проблемами", brands_with_errors)

# Error types analysis
st.subheader("🔍 Анализ типов ошибок")

if error_types:
    # Show top error types
    with st.expander("Топ-10 наиболее частых ошибок", expanded=True):
        top_errors_df = pd.DataFrame(error_types[:10], columns=['Тип ошибки', 'Количество'])
        st.dataframe(top_errors_df, use_container_width=True, hide_index=True)
        
        # Bar chart
        st.bar_chart(top_errors_df.set_index('Тип ошибки')['Количество'])

# Filtering section
st.subheader("🎯 Фильтрация товаров по типам ошибок")

# Create filter options
col1, col2 = st.columns([2, 1])

with col1:
    # Error type filter
    error_options = ['Все ошибки'] + [error_type for error_type, count in error_types]
    selected_error_types = st.multiselect(
        "Выберите типы ошибок для фильтрации:",
        options=error_options,
        default=['Все ошибки'],
        help="Выберите конкретные типы ошибок или оставьте 'Все ошибки' для просмотра всех"
    )

with col2:
    # Brand filter
    brands = sorted(error_df['oz_brand'].dropna().unique())
    selected_brands = st.multiselect(
        "Фильтр по брендам:",
        options=brands,
        help="Оставьте пустым для всех брендов"
    )

# Additional filters
col3, col4 = st.columns(2)
with col3:
    price_filter = st.checkbox("Фильтр по цене")
    if price_filter:
        min_price, max_price = st.slider(
            "Диапазон цен (руб.):",
            min_value=0,
            max_value=int(error_df['oz_actual_price'].max()) if not error_df['oz_actual_price'].isna().all() else 100000,
            value=(0, int(error_df['oz_actual_price'].max()) if not error_df['oz_actual_price'].isna().all() else 100000),
            step=100
        )

with col4:
    show_warnings = st.checkbox("Показать также предупреждения", help="Включить товары с предупреждениями")

# Apply filters
filtered_df = error_df.copy()

# Filter by error types
if 'Все ошибки' not in selected_error_types and selected_error_types:
    mask = pd.Series([False] * len(filtered_df))
    for selected_error in selected_error_types:
        # Check if the selected error type is present in the error field
        error_mask = filtered_df['error'].str.contains(re.escape(selected_error), case=False, na=False)
        mask = mask | error_mask
    filtered_df = filtered_df[mask]

# Filter by brands
if selected_brands:
    filtered_df = filtered_df[filtered_df['oz_brand'].isin(selected_brands)]

# Filter by price
if price_filter:
    filtered_df = filtered_df[
        (filtered_df['oz_actual_price'] >= min_price) & 
        (filtered_df['oz_actual_price'] <= max_price)
    ]

# Show warnings
if show_warnings:
    warning_query = """
        SELECT 
            oz_vendor_code,
            product_name,
            oz_brand,
            color,
            russian_size,
            error,
            warning,
            oz_actual_price,
            oz_sku
        FROM oz_category_products 
        WHERE warning IS NOT NULL AND warning != '' AND warning != 'NULL'
        ORDER BY oz_vendor_code
    """
    warning_df = db_connection.execute(warning_query).fetchdf()
    if not warning_df.empty:
        filtered_df = pd.concat([filtered_df, warning_df]).drop_duplicates()

# Display filtered results
st.subheader(f"📋 Результаты фильтрации ({len(filtered_df)} товаров)")

if not filtered_df.empty:
    # Show summary of filtered data
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Найдено товаров", len(filtered_df))
    with col2:
        avg_price = filtered_df['oz_actual_price'].mean()
        st.metric("Средняя цена", f"{avg_price:.0f} ₽" if not pd.isna(avg_price) else "N/A")
    with col3:
        unique_brands = filtered_df['oz_brand'].nunique()
        st.metric("Уникальных брендов", unique_brands)

    # Display options
    display_options = st.radio(
        "Выберите режим отображения:",
        ["Таблица", "Детальный вид", "Экспорт данных"],
        horizontal=True
    )

    if display_options == "Таблица":
        # Display as table
        display_columns = ['oz_vendor_code', 'product_name', 'oz_brand', 'color', 'russian_size', 'oz_actual_price', 'error']
        st.dataframe(
            filtered_df[display_columns], 
            use_container_width=True, 
            hide_index=True,
            column_config={
                'oz_vendor_code': 'Артикул',
                'product_name': 'Название товара',
                'oz_brand': 'Бренд',
                'color': 'Цвет',
                'russian_size': 'Размер',
                'oz_actual_price': st.column_config.NumberColumn('Цена, ₽', format="%.0f"),
                'error': 'Ошибки'
            }
        )

    elif display_options == "Детальный вид":
        # Detailed view with expandable cards
        st.write("Выберите товар для детального просмотра:")
        
        for idx, row in filtered_df.head(20).iterrows():  # Limit to first 20 for performance
            with st.expander(f"🏷️ {row['oz_vendor_code']} - {row['product_name'][:50]}..."):
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Артикул:** {row['oz_vendor_code']}")
                    st.write(f"**Бренд:** {row['oz_brand']}")
                    st.write(f"**Цвет:** {row['color']}")
                    st.write(f"**Размер:** {row['russian_size']}")
                    st.write(f"**Цена:** {row['oz_actual_price']} ₽")
                    if row['oz_sku']:
                        st.write(f"**SKU:** {row['oz_sku']}")
                
                with col2:
                    st.write(f"**Название:** {row['product_name']}")
                    
                if row['error']:
                    st.error(f"**Ошибки:** {row['error']}")
                    
                if row['warning']:
                    st.warning(f"**Предупреждения:** {row['warning']}")

    elif display_options == "Экспорт данных":
        # Export functionality
        st.write("### 📤 Экспорт отфильтрованных данных")
        
        # Prepare export data
        export_df = filtered_df.copy()
        
        # CSV export
        csv_data = export_df.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            label="📊 Скачать как CSV",
            data=csv_data,
            file_name=f"ozon_card_problems_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
        
        # Excel export option
        st.info("💡 Для экспорта в Excel используйте CSV формат - он поддерживается всеми версиями Excel")
        
        # Show preview
        st.write("**Предпросмотр экспортируемых данных:**")
        st.dataframe(export_df.head(10), use_container_width=True)

else:
    st.info("По выбранным фильтрам товары не найдены. Попробуйте изменить критерии поиска.")

# Error analysis by brand
if not error_df.empty:
    st.subheader("📈 Анализ ошибок по брендам")
    
    # Calculate error statistics by brand
    brand_error_stats = error_df.groupby('oz_brand').agg({
        'oz_vendor_code': 'count',
        'oz_actual_price': 'mean'
    }).round(0).rename(columns={
        'oz_vendor_code': 'Количество товаров с ошибками',
        'oz_actual_price': 'Средняя цена'
    })
    
    brand_error_stats = brand_error_stats.sort_values('Количество товаров с ошибками', ascending=False)
    
    st.dataframe(
        brand_error_stats.head(10),
        use_container_width=True,
        column_config={
            'Количество товаров с ошибками': st.column_config.NumberColumn(format="%d"),
            'Средняя цена': st.column_config.NumberColumn('Средняя цена, ₽', format="%.0f")
        }
    )

# Tips and recommendations
st.subheader("💡 Рекомендации по устранению ошибок")
st.info("""
**Как использовать этот анализ:**

1. **Приоритизация:** Начните с исправления наиболее частых ошибок
2. **Брендинг:** Обратите внимание на бренды с высоким процентом ошибок
3. **Ценовая политика:** Проверьте корреляцию между ценой и количеством ошибок
4. **Экспорт:** Используйте функцию экспорта для работы с данными в Excel
5. **Мониторинг:** Регулярно обновляйте данные и отслеживайте прогресс

**Частые типы ошибок и способы их устранения:**
- Проблемы с фото: Загрузите изображения правильного разрешения
- Неверные атрибуты: Проверьте соответствие характеристик товара
- Отсутствующие данные: Заполните обязательные поля
""")

# Close database connection
db_connection.close() 