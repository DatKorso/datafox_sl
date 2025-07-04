"""
Streamlit page for analyzing card problems in Ozon products.

This page allows users to:
- View and analyze the 'error' field from oz_category_products table
- Parse error messages that are separated by ";" symbol
- Filter products by specific error types
- Get statistics and insights about different error types
- Export filtered data
- Compare color name discrepancies within WB SKU groups

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

# Create tabs for different functionality
tab1, tab2 = st.tabs(["🚨 Анализ ошибок карточек", "🔍 Анализ расхождений параметров"])

# Tab 1: Card errors analysis
with tab1:
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
    else:
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
                help="Выберите конкретные типы ошибок или оставьте 'Все ошибки' для просмотра всех",
                key="error_types_filter"
            )

        with col2:
            # Brand filter
            brands = sorted(error_df['oz_brand'].dropna().unique())
            selected_brands = st.multiselect(
                "Фильтр по брендам:",
                options=brands,
                help="Оставьте пустым для всех брендов",
                key="error_brands_filter"
            )

        # Additional filters
        col3, col4 = st.columns(2)
        with col3:
            price_filter = st.checkbox("Фильтр по цене", key="error_price_filter")
            if price_filter:
                min_price, max_price = st.slider(
                    "Диапазон цен (руб.):",
                    min_value=0,
                    max_value=int(error_df['oz_actual_price'].max()) if not error_df['oz_actual_price'].isna().all() else 100000,
                    value=(0, int(error_df['oz_actual_price'].max()) if not error_df['oz_actual_price'].isna().all() else 100000),
                    step=100,
                    key="error_price_range"
                )

        with col4:
            show_warnings = st.checkbox("Показать также предупреждения", help="Включить товары с предупреждениями", key="error_show_warnings")

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
                horizontal=True,
                key="error_display_options"
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

# Tab 2: Field discrepancy analysis  
with tab2:
    st.header("🔍 Универсальный инструмент сравнения расхождений параметров товаров")
    st.info("""
    **Цель инструмента:** Найти товары в рамках одного WB SKU, у которых отличаются значения выбранных параметров из таблицы `oz_category_products`.
    Это поможет выявить inconsistent данные и исправить их для улучшения качества карточек.
    
    **Возможности:**
    - Анализ расхождений по любым полям: цвета, размеры, материалы, бренды и т.д.
    - Массовая стандартизация найденных расхождений
    - Экспорт результатов для корректировки данных
    """)
    
    # Initialize session state for field analysis results
    if 'field_analysis_completed' not in st.session_state:
        st.session_state.field_analysis_completed = False
    if 'field_analysis_statistics' not in st.session_state:
        st.session_state.field_analysis_statistics = {}
    if 'field_analysis_discrepancies_df' not in st.session_state:
        st.session_state.field_analysis_discrepancies_df = pd.DataFrame()
    if 'field_analysis_details_df' not in st.session_state:
        st.session_state.field_analysis_details_df = pd.DataFrame()
    if 'selected_fields_for_analysis' not in st.session_state:
        st.session_state.selected_fields_for_analysis = ['color_name']

    # Universal field discrepancy analysis functions
    @st.cache_data
    def find_field_discrepancies_for_wb_skus(_db_connection, wb_skus_list, selected_fields):
        """
        Анализирует расхождения в выбранных полях для товаров, связанных с указанными WB SKU.
        
        Args:
            _db_connection: Соединение с базой данных
            wb_skus_list: Список WB SKU для анализа
            selected_fields: Список полей для анализа расхождений
            
        Returns:
            Tuple: (статистика, DataFrame с расхождениями, DataFrame с деталями)
        """
        from utils.cross_marketplace_linker import CrossMarketplaceLinker
        
        if not wb_skus_list or not selected_fields:
            return {}, pd.DataFrame(), pd.DataFrame()
        
        try:
            # Используем CrossMarketplaceLinker для получения связей
            linker = CrossMarketplaceLinker(_db_connection)
            
            # Получаем базовые связи между WB и Ozon
            linked_df = linker.get_bidirectional_links(wb_skus=wb_skus_list)
            
            if linked_df.empty:
                st.warning("Не найдено связей между указанными WB SKU и товарами Ozon")
                return {}, pd.DataFrame(), pd.DataFrame()
            
            # Получаем oz_vendor_code для поиска в oz_category_products
            oz_vendor_codes = linked_df['oz_vendor_code'].unique().tolist()
            
            if not oz_vendor_codes:
                st.warning("Не найдено Ozon vendor codes для указанных WB SKU")
                return {}, pd.DataFrame(), pd.DataFrame()
            
            # Формируем динамический запрос на основе выбранных полей
            fields_to_select = ['oz_vendor_code', 'product_name', 'oz_brand', 'oz_actual_price'] + selected_fields
            fields_str = ', '.join([f'ocp.{field}' for field in fields_to_select])
            
            # Создаем условия для фильтрации null значений по всем выбранным полям
            field_conditions = []
            for field in selected_fields:
                field_conditions.append(f"""
                    (ocp.{field} IS NOT NULL 
                     AND TRIM(CAST(ocp.{field} AS VARCHAR)) != ''
                     AND TRIM(CAST(ocp.{field} AS VARCHAR)) != 'NULL')
                """)
            
            fields_condition = ' OR '.join(field_conditions)
            
            fields_query = f"""
            SELECT DISTINCT
                {fields_str}
            FROM oz_category_products ocp
            WHERE ocp.oz_vendor_code IN ({', '.join(['?'] * len(oz_vendor_codes))})
                AND ({fields_condition})
            ORDER BY ocp.oz_vendor_code
            """
            
            fields_df = _db_connection.execute(fields_query, oz_vendor_codes).fetchdf()
            
            if fields_df.empty:
                st.warning(f"Не найдено данных по выбранным полям для связанных товаров Ozon")
                return {}, pd.DataFrame(), pd.DataFrame()
            
            # Объединяем данные о связях с данными о полях
            merged_df = pd.merge(
                linked_df[['wb_sku', 'oz_vendor_code']], 
                fields_df, 
                on='oz_vendor_code', 
                how='inner'
            )
            
            if merged_df.empty:
                return {}, pd.DataFrame(), pd.DataFrame()
            
            # Анализируем расхождения по wb_sku для каждого выбранного поля
            discrepancies = []
            all_details = []
            field_discrepancy_summary = {}
            
            for wb_sku in merged_df['wb_sku'].unique():
                wb_sku_data = merged_df[merged_df['wb_sku'] == wb_sku]
                
                # Анализируем каждое поле на предмет расхождений
                has_any_discrepancy = False
                field_discrepancies = {}
                
                for field in selected_fields:
                    if field in wb_sku_data.columns:
                        # Получаем уникальные значения для этого поля
                        unique_values = wb_sku_data[field].dropna().unique()
                        unique_values = [str(v).strip() for v in unique_values 
                                       if str(v).strip() and str(v).strip().upper() != 'NULL']
                        
                        if len(unique_values) > 1:
                            has_any_discrepancy = True
                            field_discrepancies[field] = unique_values
                            
                            # Подсчитываем статистику по полям
                            if field not in field_discrepancy_summary:
                                field_discrepancy_summary[field] = 0
                            field_discrepancy_summary[field] += 1
                
                if has_any_discrepancy:
                    # Есть расхождения в одном или нескольких полях
                    discrepancy_details = []
                    for field, values in field_discrepancies.items():
                        discrepancy_details.append(f"{field}: {'; '.join(values)}")
                    
                    discrepancies.append({
                        'wb_sku': wb_sku,
                        'fields_with_discrepancies': list(field_discrepancies.keys()),
                        'discrepancy_details': ' | '.join(discrepancy_details),
                        'oz_products_count': len(wb_sku_data),
                        'unique_oz_vendor_codes': len(wb_sku_data['oz_vendor_code'].unique())
                    })
                
                # Добавляем детали для всех товаров этого wb_sku
                for _, row in wb_sku_data.iterrows():
                    detail_row = {
                        'wb_sku': row['wb_sku'],
                        'oz_vendor_code': row['oz_vendor_code'],
                        'product_name': row['product_name'],
                        'oz_brand': row['oz_brand'],
                        'oz_actual_price': row['oz_actual_price'],
                        'has_discrepancy': 'Да' if has_any_discrepancy else 'Нет'
                    }
                    
                    # Добавляем значения выбранных полей
                    for field in selected_fields:
                        if field in row.index:
                            detail_row[field] = row[field]
                    
                    all_details.append(detail_row)
            
            # Создаем DataFrame с расхождениями
            discrepancies_df = pd.DataFrame(discrepancies)
            details_df = pd.DataFrame(all_details)
            
            # Статистика
            total_wb_skus = len(wb_skus_list)
            analyzed_wb_skus = len(merged_df['wb_sku'].unique())
            wb_skus_with_discrepancies = len(discrepancies_df)
            wb_skus_without_discrepancies = analyzed_wb_skus - wb_skus_with_discrepancies
            
            statistics = {
                'total_wb_skus_requested': total_wb_skus,
                'analyzed_wb_skus': analyzed_wb_skus,
                'wb_skus_with_discrepancies': wb_skus_with_discrepancies,
                'wb_skus_without_discrepancies': wb_skus_without_discrepancies,
                'discrepancy_percentage': (wb_skus_with_discrepancies / analyzed_wb_skus * 100) if analyzed_wb_skus > 0 else 0,
                'selected_fields': selected_fields,
                'field_discrepancy_summary': field_discrepancy_summary,
                'total_products_analyzed': len(details_df)
            }
            
            return statistics, discrepancies_df, details_df
            
        except Exception as e:
            st.error(f"Ошибка при анализе расхождений полей: {e}")
            return {}, pd.DataFrame(), pd.DataFrame()

    # Field selection section
    st.subheader("🎯 Выбор полей для анализа расхождений")
    
    # Define field categories for better organization
    field_categories = {
        "🎨 Характеристики товара": [
            'color_name', 'color', 'russian_size', 'manufacturer_size', 
            'type', 'gender', 'season', 'merge_on_card', 'is_18plus'
        ],
        "🧵 Материалы": [
            'material', 'upper_material', 'lining_material', 
            'insole_material', 'outsole_material'
        ],
        "🏷️ Брендинг и категории": [
            'oz_brand', 'collection', 'style', 'group_name'
        ],
        "📏 Размеры и параметры": [
            'fullness', 'heel_height_cm', 'sole_height_cm', 
            'bootleg_height_cm', 'platform_height_cm', 'foot_length_cm',
            'insole_length_cm', 'size_info'
        ],
        "⚡ Специальные характеристики": [
            'orthopedic', 'waterproof', 'sport_purpose', 
            'target_audience', 'temperature_mode', 'pronation_type',
            'membrane_material_type'
        ],
        "🔧 Технические детали": [
            'fastener_type', 'heel_type', 'model_features', 
            'decorative_elements', 'fit', 'boots_model', 'shoes_model', 
            'ballet_flats_model'
        ],
        "🌍 Географические данные": [
            'country_of_origin', 'brand_country'
        ],
        "📸 Медиа и контент": [
            'main_photo_url', 'additional_photos_urls', 'photo_360_urls', 
            'photo_article', 'hashtags', 'annotation', 'rich_content_json', 
            'keywords'
        ],
        "📦 Товарная информация": [
            'product_name', 'oz_sku', 'barcode', 'oz_actual_price', 
            'oz_price_before_discount', 'vat_percent', 'installment', 
            'review_points'
        ],
        "📐 Упаковка и логистика": [
            'package_weight_g', 'package_width_mm', 'package_height_mm', 
            'package_length_mm', 'package_count', 'shoes_in_pack_count'
        ],
        "📋 Техническая документация": [
            'size_table_json', 'warranty_period', 'tnved_codes', 
            'error', 'warning'
        ]
    }
    
    # Create expandable sections for field selection
    selected_fields = []
    
    # Quick selection presets
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        if st.button("🎨 Только цвета", help="Выбрать поля связанные с цветами"):
            st.session_state.selected_fields_for_analysis = ['color_name', 'color']
    with col2:
        if st.button("📏 Размеры", help="Выбрать поля связанные с размерами"):
            st.session_state.selected_fields_for_analysis = ['russian_size', 'manufacturer_size', 'fullness']
    with col3:
        if st.button("🧵 Материалы", help="Выбрать поля связанные с материалами"):
            st.session_state.selected_fields_for_analysis = ['material', 'upper_material', 'lining_material']
    with col4:
        if st.button("📸 Медиа", help="Выбрать поля связанные с фото и контентом"):
            st.session_state.selected_fields_for_analysis = ['main_photo_url', 'additional_photos_urls', 'photo_360_urls']
    
    # Manual field selection
    with st.expander("🔧 Ручной выбор полей", expanded=True):
        for category_name, fields in field_categories.items():
            st.write(f"**{category_name}**")
            cols = st.columns(3)
            for i, field in enumerate(fields):
                with cols[i % 3]:
                    if st.checkbox(
                        field,
                        value=field in st.session_state.selected_fields_for_analysis,
                        key=f"field_checkbox_{field}"
                    ):
                        if field not in selected_fields:
                            selected_fields.append(field)
                    elif field in st.session_state.selected_fields_for_analysis:
                        # Remove field if unchecked
                        new_list = [f for f in st.session_state.selected_fields_for_analysis if f != field]
                        st.session_state.selected_fields_for_analysis = new_list
    
    # Update selected fields
    if selected_fields:
        # Add newly selected fields
        for field in selected_fields:
            if field not in st.session_state.selected_fields_for_analysis:
                st.session_state.selected_fields_for_analysis.append(field)
    
    # Display selected fields
    if st.session_state.selected_fields_for_analysis:
        st.success(f"Выбрано полей: {', '.join(st.session_state.selected_fields_for_analysis)}")
    else:
        st.warning("Выберите хотя бы одно поле для анализа")
        
    st.divider()
    
    # Input section for WB SKUs
    st.subheader("📝 Ввод WB SKU для анализа")

    col1, col2 = st.columns([3, 1])

    with col1:
        wb_skus_input = st.text_area(
            "Введите WB SKU (по одному на строку или через запятой):",
            height=100,
            placeholder="Например:\n12345\n67890\nили: 12345, 67890, 54321",
            help="Введите артикулы WB для поиска связанных товаров Ozon и анализа расхождений в выбранных полях",
            key="field_wb_skus_input"
        )

    with col2:
        st.write("**Примеры:**")
        st.code("12345\n67890\n54321")
        st.write("или")
        st.code("12345, 67890, 54321")

    if st.button("🔍 Анализировать расхождения", type="primary", key="field_analyze_button"):
        if not wb_skus_input.strip():
            st.warning("Пожалуйста, введите хотя бы один WB SKU")
        elif not st.session_state.selected_fields_for_analysis:
            st.warning("Пожалуйста, выберите хотя бы одно поле для анализа")
        else:
            # Парсим введенные WB SKU
            wb_skus_text = wb_skus_input.strip()
            
            # Разделяем по запятым, затем по новым строкам
            if ',' in wb_skus_text:
                wb_skus_raw = [sku.strip() for sku in wb_skus_text.split(',')]
            else:
                wb_skus_raw = [sku.strip() for sku in wb_skus_text.split('\n')]
            
            # Очищаем и валидируем SKU
            wb_skus_list = []
            for sku in wb_skus_raw:
                sku = sku.strip()
                if sku and sku.isdigit():
                    wb_skus_list.append(sku)
                elif sku:
                    st.warning(f"WB SKU '{sku}' не является числом и будет пропущен")
            
            if not wb_skus_list:
                st.error("Не найдено валидных WB SKU для анализа")
            else:
                st.success(f"Найдено {len(wb_skus_list)} валидных WB SKU для анализа")
                
                with st.spinner(f"Анализируем расхождения в полях: {', '.join(st.session_state.selected_fields_for_analysis)}..."):
                    statistics, discrepancies_df, details_df = find_field_discrepancies_for_wb_skus(
                        db_connection, wb_skus_list, st.session_state.selected_fields_for_analysis
                    )
                    
                    # Save results to session state
                    if statistics:
                        st.session_state.field_analysis_completed = True
                        st.session_state.field_analysis_statistics = statistics
                        st.session_state.field_analysis_discrepancies_df = discrepancies_df
                        st.session_state.field_analysis_details_df = details_df
                    else:
                        st.session_state.field_analysis_completed = False
    
    # Add button to clear analysis results if analysis was completed
    if st.session_state.field_analysis_completed:
        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("🔄 Очистить результаты", key="clear_field_analysis"):
                st.session_state.field_analysis_completed = False
                st.session_state.field_analysis_statistics = {}
                st.session_state.field_analysis_discrepancies_df = pd.DataFrame()
                st.session_state.field_analysis_details_df = pd.DataFrame()
                st.rerun()
    
    # Display results from session state
    if st.session_state.field_analysis_completed and st.session_state.field_analysis_statistics:
        # Отображаем статистику
        st.subheader("📊 Статистика анализа")
        
        statistics = st.session_state.field_analysis_statistics
        discrepancies_df = st.session_state.field_analysis_discrepancies_df
        details_df = st.session_state.field_analysis_details_df
        
        # Основные метрики
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Всего WB SKU", statistics['total_wb_skus_requested'])
        with col2:
            st.metric("Проанализировано", statistics['analyzed_wb_skus'])
        with col3:
            st.metric("С расхождениями", statistics['wb_skus_with_discrepancies'])
        with col4:
            st.metric("Без расхождений", statistics['wb_skus_without_discrepancies'])
        with col5:
            st.metric("Всего товаров", statistics['total_products_analyzed'])
        
        # Процент расхождений и дополнительная информация
        col1, col2 = st.columns(2)
        with col1:
            if statistics['analyzed_wb_skus'] > 0:
                st.metric(
                    "Процент с расхождениями", 
                    f"{statistics['discrepancy_percentage']:.1f}%"
                )
        with col2:
            st.info(f"**Анализируемые поля:** {', '.join(statistics['selected_fields'])}")
        
        # Статистика по полям
        if statistics['field_discrepancy_summary']:
            st.subheader("📈 Распределение расхождений по полям")
            field_stats_df = pd.DataFrame(
                list(statistics['field_discrepancy_summary'].items()),
                columns=['Поле', 'Количество WB SKU с расхождениями']
            ).sort_values('Количество WB SKU с расхождениями', ascending=False)
            
            col1, col2 = st.columns([2, 1])
            with col1:
                st.dataframe(field_stats_df, use_container_width=True, hide_index=True)
            with col2:
                st.bar_chart(field_stats_df.set_index('Поле')['Количество WB SKU с расхождениями'])
        
        # Результаты анализа
        if not discrepancies_df.empty:
            st.subheader("⚠️ WB SKU с расхождениями в анализируемых полях")
            
            # Настройка колонок для отображения
            column_config = {
                'wb_sku': 'WB SKU',
                'fields_with_discrepancies': 'Поля с расхождениями',
                'discrepancy_details': st.column_config.TextColumn('Детали расхождений', width="large"),
                'oz_products_count': st.column_config.NumberColumn('Всего товаров Ozon', format="%d"),
                'unique_oz_vendor_codes': st.column_config.NumberColumn('Уникальных артикулов', format="%d")
            }
            
            st.dataframe(
                discrepancies_df,
                use_container_width=True,
                hide_index=True,
                column_config=column_config
            )
            
            # Детальный просмотр расхождений
            st.subheader("🔍 Детальный просмотр расхождений")
            
            # Фильтр по WB SKU с расхождениями
            selected_wb_sku = st.selectbox(
                "Выберите WB SKU для детального просмотра:",
                options=discrepancies_df['wb_sku'].tolist(),
                help="Выберите WB SKU для просмотра всех связанных товаров Ozon",
                key="field_selected_wb_sku"
            )
            
            if selected_wb_sku:
                selected_details = details_df[details_df['wb_sku'] == selected_wb_sku]
                
                if not selected_details.empty:
                    st.write(f"**Товары Ozon для WB SKU {selected_wb_sku}:**")
                    
                    # Динамически создаем конфигурацию колонок
                    detail_column_config = {
                        'wb_sku': 'WB SKU',
                        'oz_vendor_code': 'Артикул Ozon',
                        'product_name': 'Название товара',
                        'oz_brand': 'Бренд',
                        'oz_actual_price': st.column_config.NumberColumn('Цена, ₽', format="%.0f"),
                        'has_discrepancy': 'Есть расхождения'
                    }
                    
                    # Добавляем конфигурации для выбранных полей
                    for field in statistics['selected_fields']:
                        if field in selected_details.columns:
                            detail_column_config[field] = field
                    
                    st.dataframe(
                        selected_details,
                        use_container_width=True,
                        hide_index=True,
                        column_config=detail_column_config
                    )
                    
                    # Показываем уникальные значения для каждого анализируемого поля
                    st.write("**Уникальные значения по полям:**")
                    for field in statistics['selected_fields']:
                        if field in selected_details.columns:
                            unique_values = selected_details[field].dropna().unique()
                            unique_values = [str(v) for v in unique_values if str(v).strip() and str(v).strip().upper() != 'NULL']
                            if unique_values:
                                if len(unique_values) > 1:
                                    st.warning(f"**{field}:** {', '.join(unique_values)} ⚠️ (расхождение)")
                                else:
                                    st.success(f"**{field}:** {', '.join(unique_values)} ✅ (единообразно)")
        else:
            st.success("🎉 Отлично! Все проанализированные WB SKU имеют согласованные значения в выбранных полях.")
        

    # Additional information about the tool
    st.subheader("ℹ️ Информация об инструменте")
    st.expander("Как работает универсальный анализ расхождений", expanded=False).write("""
    **Алгоритм работы:**
    
    1. **Выбор полей:** Пользователь выбирает поля из таблицы `oz_category_products` для анализа
    2. **Поиск связей:** Для каждого введенного WB SKU ищутся связанные товары Ozon через общие штрихкоды
    3. **Извлечение данных:** Из таблицы `oz_category_products` извлекаются данные по выбранным полям
    4. **Анализ расхождений:** Для каждого WB SKU проверяется, есть ли разные значения в любом из выбранных полей
    5. **Статистика:** Подсчитывается статистика по каждому полю и общая статистика расхождений
    6. **Детализация:** Предоставляется подробная информация о каждом найденном расхождении
    
    **Типы полей для анализа:**
    - **Характеристики товара:** цвета, размеры, типы, пол, сезон
    - **Материалы:** основной материал, материал верха, подкладки, стельки, подошвы
    - **Брендинг:** бренд, коллекция, стиль, группа товаров
    - **Технические параметры:** застежки, каблуки, особенности модели
    - **Географические данные:** страна производства, страна бренда
    
    **Польза анализа:**
    - Выявление inconsistent данных в любых полях карточек товаров
    - Улучшение качества данных для лучшей конверсии и SEO
    - Стандартизация характеристик в рамках одного товарного ряда
    - Поиск потенциальных ошибок в заполнении любых полей карточек
    - Массовое исправление найденных расхождений
    """)

    # Анализ расхождений внешних кодов
    st.divider()
    st.header("🔍 Анализ расхождений внешних кодов артикулов")
    st.info("""
    **Цель анализа:** Найти WB SKU, которые имеют более 2 разных внешних кодов в артикулах поставщика.
    
    **Структура артикула поставщика (oz_vendor_code):**
    - **1 часть:** ВНЕШНИЙ КОД (с коробки товара)
    - **2 часть:** цвет товара
    - **3 часть:** размер товара
    
    **Пример:** `123123-черный-32`, `123123-черный-33`, `321321-черный-34`
    
    **Логика анализа:**
    - Извлекается внешний код (первая часть до первого дефиса)
    - Подсчитываются уникальные внешние коды для каждого WB SKU
    - Сообщается о расхождениях только если более 2 разных внешних кодов
    """)

    # Initialize session state for external code analysis
    if 'external_code_analysis_completed' not in st.session_state:
        st.session_state.external_code_analysis_completed = False
    if 'external_code_analysis_results' not in st.session_state:
        st.session_state.external_code_analysis_results = {}

    @st.cache_data
    def analyze_external_codes_discrepancies(_db_connection):
        """
        Анализирует расхождения во внешних кодах артикулов поставщика в пределах каждого WB SKU.
        
        Args:
            _db_connection: Соединение с базой данных
            
        Returns:
            Dict: Результаты анализа с статистикой и данными о расхождениях
        """
        try:
            from utils.cross_marketplace_linker import CrossMarketplaceLinker
            
            # Получаем все связи между WB и Ozon
            linker = CrossMarketplaceLinker(_db_connection)
            all_links_df = linker.get_bidirectional_links()
            
            if all_links_df.empty:
                st.warning("Не найдено связей между WB и Ozon товарами")
                return {}
            
            # Получаем данные о товарах Ozon
            oz_vendor_codes = all_links_df['oz_vendor_code'].unique().tolist()
            
            if not oz_vendor_codes:
                st.warning("Не найдено Ozon vendor codes")
                return {}
            
            # Запрос для получения данных из oz_category_products
            products_query = f"""
            SELECT DISTINCT
                ocp.oz_vendor_code,
                ocp.product_name,
                ocp.oz_brand,
                ocp.color,
                ocp.russian_size,
                ocp.oz_actual_price
            FROM oz_category_products ocp
            WHERE ocp.oz_vendor_code IN ({', '.join(['?'] * len(oz_vendor_codes))})
                AND ocp.oz_vendor_code IS NOT NULL 
                AND TRIM(ocp.oz_vendor_code) != ''
                AND TRIM(ocp.oz_vendor_code) != 'NULL'
            ORDER BY ocp.oz_vendor_code
            """
            
            products_df = _db_connection.execute(products_query, oz_vendor_codes).fetchdf()
            
            if products_df.empty:
                st.warning("Не найдено данных о товарах Ozon")
                return {}
            
            # Объединяем данные о связях с данными о товарах
            merged_df = pd.merge(
                all_links_df[['wb_sku', 'oz_vendor_code']], 
                products_df, 
                on='oz_vendor_code', 
                how='inner'
            )
            
            if merged_df.empty:
                return {}
            
            # Извлекаем внешний код из oz_vendor_code (до первого дефиса)
            def extract_external_code(vendor_code):
                """Извлекает внешний код из артикула поставщика"""
                if pd.isna(vendor_code) or not str(vendor_code).strip():
                    return None
                
                vendor_code_str = str(vendor_code).strip()
                
                # Ищем первый дефис
                if '-' in vendor_code_str:
                    return vendor_code_str.split('-')[0].strip()
                else:
                    # Если дефиса нет, считаем весь код внешним
                    return vendor_code_str
            
            # Добавляем столбец с внешним кодом
            merged_df['external_code'] = merged_df['oz_vendor_code'].apply(extract_external_code)
            
            # Убираем записи без внешнего кода
            merged_df = merged_df[merged_df['external_code'].notna()]
            merged_df = merged_df[merged_df['external_code'] != '']
            
            if merged_df.empty:
                st.warning("Не найдено валидных внешних кодов в артикулах")
                return {}
            
            # Анализируем расхождения по wb_sku
            wb_sku_analysis = []
            discrepancy_details = []
            
            for wb_sku in merged_df['wb_sku'].unique():
                wb_sku_data = merged_df[merged_df['wb_sku'] == wb_sku]
                
                # Получаем уникальные внешние коды для этого WB SKU
                unique_external_codes = wb_sku_data['external_code'].unique()
                unique_external_codes = [code for code in unique_external_codes if code and str(code).strip()]
                
                # Анализируем только если более 2 разных внешних кодов
                if len(unique_external_codes) > 2:
                    wb_sku_analysis.append({
                        'wb_sku': wb_sku,
                        'external_codes_count': len(unique_external_codes),
                        'external_codes': unique_external_codes,
                        'external_codes_str': ', '.join(unique_external_codes),
                        'products_count': len(wb_sku_data),
                        'unique_oz_vendor_codes': len(wb_sku_data['oz_vendor_code'].unique()),
                        'brands': wb_sku_data['oz_brand'].unique().tolist()
                    })
                    
                    # Добавляем детали для каждого товара этого wb_sku
                    for _, row in wb_sku_data.iterrows():
                        discrepancy_details.append({
                            'wb_sku': row['wb_sku'],
                            'oz_vendor_code': row['oz_vendor_code'],
                            'external_code': row['external_code'],
                            'product_name': row['product_name'],
                            'oz_brand': row['oz_brand'],
                            'color': row['color'],
                            'russian_size': row['russian_size'],
                            'oz_actual_price': row['oz_actual_price'],
                            'has_discrepancy': 'Да'
                        })
            
            # Создаем DataFrame с результатами
            discrepancies_df = pd.DataFrame(wb_sku_analysis)
            details_df = pd.DataFrame(discrepancy_details)
            
            # Статистика
            total_wb_skus = len(merged_df['wb_sku'].unique())
            wb_skus_with_discrepancies = len(discrepancies_df)
            total_products = len(merged_df)
            
            statistics = {
                'total_wb_skus_analyzed': total_wb_skus,
                'wb_skus_with_discrepancies': wb_skus_with_discrepancies,
                'wb_skus_without_discrepancies': total_wb_skus - wb_skus_with_discrepancies,
                'discrepancy_percentage': (wb_skus_with_discrepancies / total_wb_skus * 100) if total_wb_skus > 0 else 0,
                'total_products_analyzed': total_products,
                'total_products_with_discrepancies': len(details_df)
            }
            
            return {
                'statistics': statistics,
                'discrepancies_df': discrepancies_df,
                'details_df': details_df,
                'all_data_df': merged_df
            }
            
        except Exception as e:
            st.error(f"Ошибка при анализе внешних кодов: {e}")
            return {}

    # Кнопка запуска анализа
    col1, col2 = st.columns([1, 3])
    
    with col1:
        if st.button("🔍 Анализировать внешние коды", type="primary", key="analyze_external_codes"):
            with st.spinner("Анализируем расхождения во внешних кодах..."):
                analysis_results = analyze_external_codes_discrepancies(db_connection)
                
                if analysis_results:
                    st.session_state.external_code_analysis_completed = True
                    st.session_state.external_code_analysis_results = analysis_results
                    st.success("✅ Анализ внешних кодов завершен!")
                else:
                    st.session_state.external_code_analysis_completed = False
    
    with col2:
        if st.session_state.external_code_analysis_completed:
            if st.button("🔄 Очистить результаты анализа", key="clear_external_code_analysis"):
                st.session_state.external_code_analysis_completed = False
                st.session_state.external_code_analysis_results = {}
                st.rerun()

    # Отображение результатов анализа внешних кодов
    if st.session_state.external_code_analysis_completed and st.session_state.external_code_analysis_results:
        results = st.session_state.external_code_analysis_results
        statistics = results.get('statistics', {})
        discrepancies_df = results.get('discrepancies_df', pd.DataFrame())
        details_df = results.get('details_df', pd.DataFrame())
        
        # Статистика анализа
        st.subheader("📊 Статистика анализа внешних кодов")
        
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Всего WB SKU", statistics.get('total_wb_skus_analyzed', 0))
        with col2:
            st.metric("С расхождениями", statistics.get('wb_skus_with_discrepancies', 0))
        with col3:
            st.metric("Без расхождений", statistics.get('wb_skus_without_discrepancies', 0))
        with col4:
            st.metric("Процент расхождений", f"{statistics.get('discrepancy_percentage', 0):.1f}%")
        with col5:
            st.metric("Товаров с проблемами", statistics.get('total_products_with_discrepancies', 0))
        
        if not discrepancies_df.empty:
            # Результаты расхождений
            st.subheader("⚠️ WB SKU с расхождениями во внешних кодах")
            
            # Конфигурация колонок для таблицы расхождений
            discrepancy_column_config = {
                'wb_sku': 'WB SKU',
                'external_codes_count': st.column_config.NumberColumn('Кол-во внешних кодов', format="%d"),
                'external_codes_str': st.column_config.TextColumn('Внешние коды', width="medium"),
                'products_count': st.column_config.NumberColumn('Всего товаров', format="%d"),
                'unique_oz_vendor_codes': st.column_config.NumberColumn('Уникальных артикулов', format="%d"),
                'brands': 'Бренды'
            }
            
            # Обработка колонки brands для отображения
            display_discrepancies_df = discrepancies_df.copy()
            display_discrepancies_df['brands'] = display_discrepancies_df['brands'].apply(
                lambda x: ', '.join(x) if isinstance(x, list) else str(x)
            )
            
            st.dataframe(
                display_discrepancies_df,
                use_container_width=True,
                hide_index=True,
                column_config=discrepancy_column_config
            )
            
            # Детальный просмотр расхождений
            st.subheader("🔍 Детальный просмотр расхождений во внешних кодах")
            
            if not discrepancies_df.empty:
                selected_wb_sku_ext = st.selectbox(
                    "Выберите WB SKU для детального просмотра:",
                    options=discrepancies_df['wb_sku'].tolist(),
                    help="Выберите WB SKU для просмотра всех связанных товаров Ozon",
                    key="external_code_selected_wb_sku"
                )
                
                if selected_wb_sku_ext:
                    selected_details = details_df[details_df['wb_sku'] == selected_wb_sku_ext]
                    
                    if not selected_details.empty:
                        st.write(f"**Товары Ozon для WB SKU {selected_wb_sku_ext}:**")
                        
                        detail_column_config = {
                            'wb_sku': 'WB SKU',
                            'oz_vendor_code': 'Артикул Ozon',
                            'external_code': st.column_config.TextColumn('Внешний код', width="medium"),
                            'product_name': 'Название товара',
                            'oz_brand': 'Бренд',
                            'color': 'Цвет',
                            'russian_size': 'Размер',
                            'oz_actual_price': st.column_config.NumberColumn('Цена, ₽', format="%.0f"),
                            'has_discrepancy': 'Есть расхождения'
                        }
                        
                        st.dataframe(
                            selected_details,
                            use_container_width=True,
                            hide_index=True,
                            column_config=detail_column_config
                        )
                        
                        # Показываем уникальные внешние коды для выбранного WB SKU
                        unique_codes = selected_details['external_code'].unique()
                        st.warning(f"**Найдено {len(unique_codes)} разных внешних кодов:** {', '.join(unique_codes)}")
                        
                        # Группировка по внешним кодам
                        st.write("**Распределение товаров по внешним кодам:**")
                        for code in unique_codes:
                            code_products = selected_details[selected_details['external_code'] == code]
                            with st.expander(f"Внешний код: {code} ({len(code_products)} товаров)"):
                                st.dataframe(
                                    code_products[['oz_vendor_code', 'color', 'russian_size', 'oz_actual_price']],
                                    use_container_width=True,
                                    hide_index=True,
                                    column_config={
                                        'oz_vendor_code': 'Артикул Ozon',
                                        'color': 'Цвет',
                                        'russian_size': 'Размер',
                                        'oz_actual_price': st.column_config.NumberColumn('Цена, ₽', format="%.0f")
                                    }
                                )
            
            # Экспорт результатов анализа внешних кодов
            st.subheader("📤 Экспорт результатов анализа внешних кодов")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Экспорт сводной таблицы расхождений
                summary_export = display_discrepancies_df.copy()
                summary_csv = summary_export.to_csv(index=False, encoding='utf-8-sig')
                
                st.download_button(
                    label="📊 Скачать сводную таблицу расхождений",
                    data=summary_csv,
                    file_name=f"external_codes_discrepancies_summary_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    key="download_external_codes_summary"
                )
            
            with col2:
                # Экспорт детальных данных
                details_csv = details_df.to_csv(index=False, encoding='utf-8-sig')
                
                st.download_button(
                    label="📋 Скачать детальные данные",
                    data=details_csv,
                    file_name=f"external_codes_discrepancies_details_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    key="download_external_codes_details"
                )
        
        else:
            st.success("🎉 Отлично! Все проанализированные WB SKU имеют не более 2 разных внешних кодов.")
    
    else:
        st.info("💡 Нажмите кнопку 'Анализировать внешние коды' для начала анализа расхождений во внешних кодах артикулов.")

    # Пояснения к анализу внешних кодов
    with st.expander("ℹ️ Подробнее об анализе внешних кодов"):
        st.write("""
        **Что анализируется:**
        
        1. **Извлечение внешнего кода:** Из каждого `oz_vendor_code` извлекается первая часть до первого дефиса
        2. **Группировка:** Товары группируются по `wb_sku`
        3. **Подсчет уникальных кодов:** Для каждого WB SKU подсчитывается количество уникальных внешних кодов
        4. **Фильтрация:** Показываются только WB SKU с более чем 2 разными внешними кодами
        
        **Примеры артикулов:**
        - `123123-черный-32` → внешний код: `123123`
        - `123123-белый-34` → внешний код: `123123`
        - `321321-красный-36` → внешний код: `321321`
        
        **Зачем это нужно:**
        - Выявление товаров с разными поставками в рамках одного WB SKU
        - Контроль качества данных и соответствия артикулов
        - Обнаружение потенциальных ошибок в связывании товаров
        
        **Что делать с найденными расхождениями:**
        - Проверить правильность связывания товаров через штрихкоды
        - Убедиться, что все товары действительно относятся к одному WB SKU
        - При необходимости скорректировать данные или разделить на разные WB SKU
        """)

    # Массовая стандартизация названий цветов
    st.divider()
    st.header("🔧 Инструмент массовой стандартизации названий цветов")
    st.info("""
    **Цель инструмента:** Автоматически генерировать стандартизированные названия цветов.
    
    **Режимы работы:**
    - **Все товары:** Обрабатывает все товары из oz_category_products, связанные с WB SKU
    - **Только расхождения:** Обрабатывает только товары с найденными расхождениями в названиях цветов
    
    **Принцип работы:**
    - Берется максимальное значение `sort` из `punta_table` для каждого WB SKU
    - К значению `sort` добавляется порядковый номер позиции в рамках этого `sort`
    - Формат результата: `{цвет}; {sort}-{позиция}`
    
    **Пример:** Если WB SKU находится на 5-й позиции в рамках sort=12, то результат: "красный; 12-5"
    """)

    # Initialize session state for standardization results
    if 'standardization_completed' not in st.session_state:
        st.session_state.standardization_completed = False
    if 'standardization_results_df' not in st.session_state:
        st.session_state.standardization_results_df = pd.DataFrame()

    # Функции для стандартизации
    @st.cache_data
    def generate_standardized_color_names(_db_connection, discrepancies_df=None, details_df=None, use_all_products=False):
        """
        Генерирует стандартизированные названия цветов на основе punta_table.sort
        
        Args:
            _db_connection: Соединение с базой данных
            discrepancies_df: DataFrame с расхождениями (используется если use_all_products=False)
            details_df: DataFrame с деталями товаров (используется если use_all_products=False)
            use_all_products: Если True, обрабатывает все товары из oz_category_products
            
        Returns:
            DataFrame с результатами стандартизации
        """
        try:
            if use_all_products:
                # Получаем все товары с данными о цветах и связи с WB через CrossMarketplaceLinker
                from utils.cross_marketplace_linker import CrossMarketplaceLinker
                
                linker = CrossMarketplaceLinker(_db_connection)
                
                # Получаем все связи
                all_links_df = linker.get_bidirectional_links()
                
                if all_links_df.empty:
                    st.warning("Не найдено связей между WB и Ozon товарами")
                    return pd.DataFrame()
                
                # Получаем oz_vendor_code для поиска в oz_category_products
                oz_vendor_codes = all_links_df['oz_vendor_code'].unique().tolist()
                
                if not oz_vendor_codes:
                    st.warning("Не найдено Ozon vendor codes")
                    return pd.DataFrame()
                
                # Запрос для получения всех данных о цветах из oz_category_products
                color_query = f"""
                SELECT DISTINCT
                    ocp.oz_vendor_code,
                    ocp.color_name,
                    ocp.product_name,
                    ocp.oz_brand,
                    ocp.color,
                    ocp.russian_size,
                    ocp.oz_actual_price
                FROM oz_category_products ocp
                WHERE ocp.oz_vendor_code IN ({', '.join(['?'] * len(oz_vendor_codes))})
                    AND ocp.color_name IS NOT NULL 
                    AND TRIM(ocp.color_name) != ''
                    AND TRIM(ocp.color_name) != 'NULL'
                ORDER BY ocp.oz_vendor_code
                """
                
                color_df = _db_connection.execute(color_query, oz_vendor_codes).fetchdf()
                
                if color_df.empty:
                    st.warning("Не найдено данных о цветах для товаров Ozon")
                    return pd.DataFrame()
                
                # Объединяем данные о связях с данными о цветах
                all_products_df = pd.merge(
                    all_links_df[['wb_sku', 'oz_vendor_code']], 
                    color_df, 
                    on='oz_vendor_code', 
                    how='inner'
                )
                
                if all_products_df.empty:
                    return pd.DataFrame()
                
                # Получаем уникальные WB SKU для обработки
                wb_skus_to_process = all_products_df['wb_sku'].unique().tolist()
                working_details_df = all_products_df
                
            else:
                # Используем данные с расхождениями (старый метод)
                if discrepancies_df is None or details_df is None or discrepancies_df.empty or details_df.empty:
                    return pd.DataFrame()
                
                # Получаем все WB SKU с расхождениями
                wb_skus_to_process = discrepancies_df['wb_sku'].unique().tolist()
                working_details_df = details_df
            
            if not wb_skus_to_process:
                return pd.DataFrame()
            
            # Запрос для получения данных из punta_table с sort и позициями
            punta_query = f"""
            WITH wb_sku_with_max_sort AS (
                -- Сначала находим максимальный sort для каждого wb_sku
                SELECT 
                    wb_sku,
                    MAX(sort) as max_sort
                FROM punta_table 
                WHERE CAST(wb_sku AS VARCHAR) IN ({', '.join(['?'] * len(wb_skus_to_process))})
                    AND sort IS NOT NULL
                    AND TRIM(CAST(sort AS VARCHAR)) != ''
                GROUP BY wb_sku
            ),
            all_wb_sku_positions AS (
                -- Для каждого sort получаем ВСЕ wb_sku из punta_table и их позиции
                SELECT 
                    wb_sku,
                    sort,
                    ROW_NUMBER() OVER (
                        PARTITION BY sort 
                        ORDER BY MIN(ROWID)
                    ) as position_in_sort_full
                FROM punta_table
                WHERE sort IS NOT NULL
                    AND TRIM(CAST(sort AS VARCHAR)) != ''
                GROUP BY wb_sku, sort
            ),
            final_positions AS (
                -- Соединяем wb_sku с их позициями в полных группах sort
                SELECT 
                    w.wb_sku,
                    w.max_sort as sort,
                    a.position_in_sort_full
                FROM wb_sku_with_max_sort w
                JOIN all_wb_sku_positions a ON CAST(w.wb_sku AS VARCHAR) = CAST(a.wb_sku AS VARCHAR) 
                    AND w.max_sort = a.sort
            )
            SELECT wb_sku, sort, position_in_sort_full as position_in_sort
            FROM final_positions
            ORDER BY sort, position_in_sort
            """
            
            punta_df = _db_connection.execute(punta_query, wb_skus_to_process).fetchdf()
            
            if punta_df.empty:
                st.warning("⚠️ После обработки не найдено валидных данных sort")
                return pd.DataFrame()
            
            # Создаем маппинг wb_sku -> стандартизированный идентификатор
            wb_sku_to_standard_id = {}
            for _, row in punta_df.iterrows():
                # Приводим wb_sku к строке и убираем .0 если есть (для совпадения с данными)
                wb_sku = str(row['wb_sku']).split('.')[0]
                sort_val = int(row['sort'])
                position = int(row['position_in_sort'])
                standard_id = f"{sort_val}-{position}"
                wb_sku_to_standard_id[wb_sku] = standard_id
            
            # Обрабатываем товары
            standardization_results = []
            
            for wb_sku in wb_skus_to_process:
                wb_sku_str = str(wb_sku)
                wb_sku_details = working_details_df[working_details_df['wb_sku'] == wb_sku_str]
                
                if wb_sku_details.empty:
                    continue
                
                # Получаем стандартизированный идентификатор
                standard_id = wb_sku_to_standard_id.get(wb_sku_str, "N/A")
                
                # Берем первый цвет как основной и обрабатываем составные цвета
                raw_color = wb_sku_details['color'].dropna().iloc[0] if not wb_sku_details['color'].dropna().empty else "Не указан"
                
                # Если цвет содержит несколько цветов разделенных ";", берем только первый
                if ";" in str(raw_color):
                    base_color = str(raw_color).split(";")[0].strip()
                else:
                    base_color = str(raw_color).strip()
                
                # Генерируем стандартизированное название цвета
                if standard_id != "N/A":
                    standardized_color_name = f"{base_color}; {standard_id}"
                else:
                    standardized_color_name = f"{base_color}; NO_SORT"
                
                # Добавляем результаты для каждого товара этого WB SKU
                for _, row in wb_sku_details.iterrows():
                    standardization_results.append({
                        'wb_sku': row['wb_sku'],
                        'oz_vendor_code': row['oz_vendor_code'],
                        'product_name': row['product_name'],
                        'current_color_name': row['color_name'],
                        'base_color': base_color,
                        'standard_id': standard_id,
                        'new_color_name': standardized_color_name,
                        'oz_brand': row['oz_brand'],
                        'russian_size': row['russian_size'],
                        'oz_actual_price': row['oz_actual_price']
                    })
            
            return pd.DataFrame(standardization_results)
            
        except Exception as e:
            st.error(f"Ошибка при генерации стандартизированных названий: {e}")
            return pd.DataFrame()

    # Раздел стандартизации (доступен всегда)
    st.subheader("🎯 Генерация стандартизированных названий цветов")
    
    # Опции выбора данных для стандартизации
    st.write("**Выберите источник данных для стандартизации:**")
    
    col1, col2 = st.columns(2)
    
    with col1:
        use_all_products = st.checkbox(
            "🌍 Использовать все товары с связанными WB SKU",
            value=False,
            help="Обработать все товары из oz_category_products, которые связаны с WB SKU",
            key="use_all_products_standardization"
        )
    
    with col2:
        can_use_discrepancies = (st.session_state.field_analysis_completed and 
                                not st.session_state.field_analysis_discrepancies_df.empty)
        
        use_only_discrepancies = st.checkbox(
            "⚠️ Использовать только товары с расхождениями",
            value=can_use_discrepancies and not use_all_products,
            disabled=not can_use_discrepancies or use_all_products,
            help="Обработать только товары с найденными расхождениями в названиях цветов",
            key="use_discrepancies_only"
        )
    
    # Информация о выбранном режиме
    if use_all_products:
        st.info("📋 **Режим: Все товары** - будут обработаны все товары из oz_category_products, связанные с WB SKU")
    elif use_only_discrepancies and can_use_discrepancies:
        discrepancies_df = st.session_state.field_analysis_discrepancies_df
        st.info(f"📋 **Режим: Только расхождения** - будет обработано {len(discrepancies_df)} WB SKU с расхождениями в выбранных полях")
    else:
        st.warning("⚠️ **Недоступно** - выберите режим обработки или выполните анализ расхождений")
    
    # Кнопка генерации (доступна если выбран валидный режим)
    can_generate = use_all_products or (use_only_discrepancies and can_use_discrepancies)
    
    col1, col2 = st.columns([1, 3])
    
    with col1:
        if st.button(
            "🔧 Генерировать стандартизированные названия", 
            type="primary", 
            disabled=not can_generate,
            key="generate_standardized_names"
        ):
            with st.spinner("Генерируем стандартизированные названия цветов..."):
                if use_all_products:
                    # Режим всех товаров
                    standardization_results = generate_standardized_color_names(
                        db_connection, 
                        use_all_products=True
                    )
                elif use_only_discrepancies:
                    # Режим только расхождений
                    discrepancies_df = st.session_state.field_analysis_discrepancies_df
                    details_df = st.session_state.field_analysis_details_df
                    standardization_results = generate_standardized_color_names(
                        db_connection, 
                        discrepancies_df=discrepancies_df, 
                        details_df=details_df,
                        use_all_products=False
                    )
                else:
                    standardization_results = pd.DataFrame()
                
                if not standardization_results.empty:
                    st.session_state.standardization_completed = True
                    st.session_state.standardization_results_df = standardization_results
                    st.success(f"✅ Генерация завершена! Обработано {len(standardization_results)} товаров.")
                else:
                    st.error("❌ Не удалось сгенерировать стандартизированные названия.")
    
    with col2:
        if st.session_state.standardization_completed:
            if st.button("🔄 Очистить результаты стандартизации", key="clear_standardization"):
                st.session_state.standardization_completed = False
                st.session_state.standardization_results_df = pd.DataFrame()
                st.rerun()
        
        # Отображаем результаты стандартизации
        if st.session_state.standardization_completed and not st.session_state.standardization_results_df.empty:
            st.subheader("📋 Результаты стандартизации")
            
            results_df = st.session_state.standardization_results_df
            
            # Статистика
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Обработано товаров", len(results_df))
            with col2:
                unique_wb_skus = results_df['wb_sku'].nunique()
                st.metric("Уникальных WB SKU", unique_wb_skus)
            with col3:
                with_sort_data = len(results_df[results_df['standard_id'] != 'NO_SORT'])
                st.metric("С данными sort", with_sort_data)
            with col4:
                no_sort_data = len(results_df[results_df['standard_id'] == 'NO_SORT'])
                st.metric("Без данных sort", no_sort_data)
            
            # Фильтры для результатов
            st.subheader("🎛️ Фильтры результатов")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                show_only_with_sort = st.checkbox("Показать только товары с данными sort", key="filter_with_sort")
            
            with col2:
                selected_wb_skus_std = st.multiselect(
                    "Фильтр по WB SKU:",
                    options=sorted(results_df['wb_sku'].unique()),
                    help="Оставьте пустым для всех WB SKU",
                    key="standardization_wb_skus_filter"
                )
            
            with col3:
                selected_brands_std = st.multiselect(
                    "Фильтр по брендам:",
                    options=sorted(results_df['oz_brand'].dropna().unique()),
                    help="Оставьте пустым для всех брендов",
                    key="standardization_brands_filter"
                )
            
            # Применяем фильтры
            filtered_results = results_df.copy()
            
            if show_only_with_sort:
                filtered_results = filtered_results[filtered_results['standard_id'] != 'NO_SORT']
            
            if selected_wb_skus_std:
                filtered_results = filtered_results[filtered_results['wb_sku'].isin(selected_wb_skus_std)]
            
            if selected_brands_std:
                filtered_results = filtered_results[filtered_results['oz_brand'].isin(selected_brands_std)]
            
            # Отображаем таблицу результатов
            st.subheader(f"📊 Таблица результатов ({len(filtered_results)} товаров)")
            
            st.dataframe(
                filtered_results,
                use_container_width=True,
                hide_index=True,
                column_config={
                    'wb_sku': 'WB SKU',
                    'oz_vendor_code': 'Артикул Ozon',
                    'product_name': 'Название товара',
                    'current_color_name': 'Текущее название цвета',
                    'base_color': 'Базовый цвет',
                    'standard_id': 'Стандартный ID',
                    'new_color_name': st.column_config.TextColumn('Новое название цвета', width="large"),
                    'oz_brand': 'Бренд',
                    'russian_size': 'Размер',
                    'oz_actual_price': st.column_config.NumberColumn('Цена, ₽', format="%.0f")
                }
            )       
            
            # Экспорт результатов
            st.subheader("📤 Экспорт результатов стандартизации")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # CSV экспорт всех результатов
                export_data = filtered_results.copy()
                csv_data = export_data.to_csv(index=False, encoding='utf-8-sig')
                
                st.download_button(
                    label="📊 Скачать полные результаты (CSV)",
                    data=csv_data,
                    file_name=f"color_standardization_full_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    key="download_full_standardization"
                )
            
            with col2:
                # CSV экспорт только для обновления (oz_vendor_code + new_color_name)
                update_data = filtered_results[['oz_vendor_code', 'new_color_name']].copy()
                update_data.columns = ['oz_vendor_code', 'color_name']
                update_csv = update_data.to_csv(index=False, encoding='utf-8-sig')
                
                st.download_button(
                    label="🔄 Скачать для обновления БД (CSV)",
                    data=update_csv,
                    file_name=f"color_standardization_update_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    key="download_update_standardization",
                    help="Файл содержит только oz_vendor_code и новые color_name для массового обновления"
                )
            
            # Инструкции по применению изменений
            st.subheader("📖 Инструкция по применению изменений")
            st.info("""
            **Для массового обновления названий цветов в базе данных:**
            
            1. **Скачайте файл "для обновления БД"** - он содержит только необходимые колонки
            2. **Проверьте данные** в Excel или другом редакторе
            3. **Примените изменения** через SQL запрос или инструмент массового обновления
            
            **Пример SQL запроса для обновления:**
            ```sql
            UPDATE oz_category_products 
            SET color_name = (SELECT color_name FROM update_table WHERE update_table.oz_vendor_code = oz_category_products.oz_vendor_code)
            WHERE oz_vendor_code IN (SELECT oz_vendor_code FROM update_table);
            ```
            
            **Важно:** Перед применением изменений создайте резервную копию данных!
            """)
        else:
            st.info("💡 Для использования инструмента стандартизации выберите один из доступных режимов обработки выше.")

# Close database connection
db_connection.close() 