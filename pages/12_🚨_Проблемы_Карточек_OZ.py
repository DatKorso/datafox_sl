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
tab1, tab2 = st.tabs(["🚨 Анализ ошибок карточек", "🔍 Сравнение расхождений цветов"])

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

# Tab 2: Color discrepancy analysis
with tab2:
    st.header("🔍 Инструмент сравнения расхождений названий цветов")
    st.info("""
    **Цель инструмента:** Найти товары в рамках одного WB SKU, у которых отличаются названия цветов в поле `color_name`.
    Это поможет выявить inconsistent данные и исправить их для улучшения качества карточек.
    """)
    
    # Initialize session state for color analysis results
    if 'color_analysis_completed' not in st.session_state:
        st.session_state.color_analysis_completed = False
    if 'color_analysis_statistics' not in st.session_state:
        st.session_state.color_analysis_statistics = {}
    if 'color_analysis_discrepancies_df' not in st.session_state:
        st.session_state.color_analysis_discrepancies_df = pd.DataFrame()
    if 'color_analysis_details_df' not in st.session_state:
        st.session_state.color_analysis_details_df = pd.DataFrame()

    # Color discrepancy analysis functions
    @st.cache_data
    def find_color_discrepancies_for_wb_skus(_db_connection, wb_skus_list):
        """
        Анализирует расхождения в названиях цветов для товаров, связанных с указанными WB SKU.
        
        Args:
            _db_connection: Соединение с базой данных
            wb_skus_list: Список WB SKU для анализа
            
        Returns:
            Tuple: (статистика, DataFrame с расхождениями, DataFrame с деталями)
        """
        from utils.cross_marketplace_linker import CrossMarketplaceLinker
        
        if not wb_skus_list:
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
            
            # Запрос для получения данных о цветах из oz_category_products
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
                st.warning("Не найдено данных о цветах для связанных товаров Ozon")
                return {}, pd.DataFrame(), pd.DataFrame()
            
            # Объединяем данные о связях с данными о цветах
            merged_df = pd.merge(
                linked_df[['wb_sku', 'oz_vendor_code']], 
                color_df, 
                on='oz_vendor_code', 
                how='inner'
            )
            
            if merged_df.empty:
                return {}, pd.DataFrame(), pd.DataFrame()
            
            # Анализируем расхождения по wb_sku
            discrepancies = []
            all_details = []
            
            for wb_sku in merged_df['wb_sku'].unique():
                wb_sku_data = merged_df[merged_df['wb_sku'] == wb_sku]
                
                # Получаем уникальные color_name для этого wb_sku
                unique_colors = wb_sku_data['color_name'].dropna().unique()
                unique_colors = [c for c in unique_colors if str(c).strip() and str(c).strip().upper() != 'NULL']
                
                if len(unique_colors) > 1:
                    # Есть расхождения
                    discrepancies.append({
                        'wb_sku': wb_sku,
                        'discrepancy_count': len(unique_colors),
                        'color_names': '; '.join(unique_colors),
                        'oz_products_count': len(wb_sku_data),
                        'unique_oz_vendor_codes': len(wb_sku_data['oz_vendor_code'].unique())
                    })
                    
                    # Добавляем детали для этого wb_sku
                    for _, row in wb_sku_data.iterrows():
                        all_details.append({
                            'wb_sku': row['wb_sku'],
                            'oz_vendor_code': row['oz_vendor_code'],
                            'product_name': row['product_name'],
                            'color_name': row['color_name'],
                            'color': row['color'],
                            'oz_brand': row['oz_brand'],
                            'russian_size': row['russian_size'],
                            'oz_actual_price': row['oz_actual_price'],
                            'has_discrepancy': 'Да'
                        })
                else:
                    # Нет расхождений, добавляем в статистику
                    for _, row in wb_sku_data.iterrows():
                        all_details.append({
                            'wb_sku': row['wb_sku'],
                            'oz_vendor_code': row['oz_vendor_code'],
                            'product_name': row['product_name'],
                            'color_name': row['color_name'],
                            'color': row['color'],
                            'oz_brand': row['oz_brand'],
                            'russian_size': row['russian_size'],
                            'oz_actual_price': row['oz_actual_price'],
                            'has_discrepancy': 'Нет'
                        })
            
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
                'discrepancy_percentage': (wb_skus_with_discrepancies / analyzed_wb_skus * 100) if analyzed_wb_skus > 0 else 0
            }
            
            return statistics, discrepancies_df, details_df
            
        except Exception as e:
            st.error(f"Ошибка при анализе расхождений цветов: {e}")
            return {}, pd.DataFrame(), pd.DataFrame()

    # Input section for WB SKUs
    st.subheader("📝 Ввод WB SKU для анализа")

    col1, col2 = st.columns([3, 1])

    with col1:
        wb_skus_input = st.text_area(
            "Введите WB SKU (по одному на строку или через запятую):",
            height=100,
            placeholder="Например:\n12345\n67890\nили: 12345, 67890, 54321",
            help="Введите артикулы WB для поиска связанных товаров Ozon и анализа расхождений в названиях цветов",
            key="color_wb_skus_input"
        )

    with col2:
        st.write("**Примеры:**")
        st.code("12345\n67890\n54321")
        st.write("или")
        st.code("12345, 67890, 54321")

    if st.button("🔍 Анализировать расхождения", type="primary", key="color_analyze_button"):
        if not wb_skus_input.strip():
            st.warning("Пожалуйста, введите хотя бы один WB SKU")
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
                
                with st.spinner("Анализируем расхождения в названиях цветов..."):
                    statistics, discrepancies_df, details_df = find_color_discrepancies_for_wb_skus(
                        db_connection, wb_skus_list
                    )
                    
                    # Save results to session state
                    if statistics:
                        st.session_state.color_analysis_completed = True
                        st.session_state.color_analysis_statistics = statistics
                        st.session_state.color_analysis_discrepancies_df = discrepancies_df
                        st.session_state.color_analysis_details_df = details_df
                    else:
                        st.session_state.color_analysis_completed = False
    
    # Add button to clear analysis results if analysis was completed
    if st.session_state.color_analysis_completed:
        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("🔄 Очистить результаты", key="clear_color_analysis"):
                st.session_state.color_analysis_completed = False
                st.session_state.color_analysis_statistics = {}
                st.session_state.color_analysis_discrepancies_df = pd.DataFrame()
                st.session_state.color_analysis_details_df = pd.DataFrame()
                st.rerun()
    
    # Display results from session state
    if st.session_state.color_analysis_completed and st.session_state.color_analysis_statistics:
        # Отображаем статистику
        st.subheader("📊 Статистика анализа")
        
        statistics = st.session_state.color_analysis_statistics
        discrepancies_df = st.session_state.color_analysis_discrepancies_df
        details_df = st.session_state.color_analysis_details_df
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Всего WB SKU", statistics['total_wb_skus_requested'])
        with col2:
            st.metric("Проанализировано", statistics['analyzed_wb_skus'])
        with col3:
            st.metric("С расхождениями", statistics['wb_skus_with_discrepancies'])
        with col4:
            st.metric("Без расхождений", statistics['wb_skus_without_discrepancies'])
        
        # Процент расхождений
        if statistics['analyzed_wb_skus'] > 0:
            st.metric(
                "Процент с расхождениями", 
                f"{statistics['discrepancy_percentage']:.1f}%"
            )
        
        # Результаты анализа
        if not discrepancies_df.empty:
            st.subheader("⚠️ WB SKU с расхождениями в названиях цветов")
            
            st.dataframe(
                discrepancies_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    'wb_sku': 'WB SKU',
                    'discrepancy_count': st.column_config.NumberColumn('Количество разных цветов', format="%d"),
                    'color_names': 'Названия цветов',
                    'oz_products_count': st.column_config.NumberColumn('Всего товаров Ozon', format="%d"),
                    'unique_oz_vendor_codes': st.column_config.NumberColumn('Уникальных артикулов', format="%d")
                }
            )
            
            # Детальный просмотр расхождений
            st.subheader("🔍 Детальный просмотр расхождений")
            
            # Фильтр по WB SKU с расхождениями
            selected_wb_sku = st.selectbox(
                "Выберите WB SKU для детального просмотра:",
                options=discrepancies_df['wb_sku'].tolist(),
                help="Выберите WB SKU для просмотра всех связанных товаров Ozon",
                key="color_selected_wb_sku"
            )
            
            if selected_wb_sku:
                selected_details = details_df[details_df['wb_sku'] == selected_wb_sku]
                
                if not selected_details.empty:
                    st.write(f"**Товары Ozon для WB SKU {selected_wb_sku}:**")
                    
                    st.dataframe(
                        selected_details,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            'wb_sku': 'WB SKU',
                            'oz_vendor_code': 'Артикул Ozon',
                            'product_name': 'Название товара',
                            'color_name': 'Название цвета',
                            'color': 'Цвет',
                            'oz_brand': 'Бренд',
                            'russian_size': 'Размер',
                            'oz_actual_price': st.column_config.NumberColumn('Цена, ₽', format="%.0f"),
                            'has_discrepancy': 'Есть расхождения'
                        }
                    )
                    
                    # Показываем уникальные названия цветов для выбранного WB SKU
                    unique_colors_for_sku = selected_details['color_name'].dropna().unique()
                    st.info(f"**Уникальные названия цветов:** {', '.join(unique_colors_for_sku)}")
        else:
            st.success("🎉 Отлично! Все проанализированные WB SKU имеют согласованные названия цветов.")
        

    # Additional information about the tool
    st.subheader("ℹ️ Информация об инструменте")
    st.expander("Как работает анализ расхождений", expanded=False).write("""
    **Алгоритм работы:**
    
    1. **Поиск связей:** Для каждого введенного WB SKU ищутся связанные товары Ozon через общие штрихкоды
    2. **Извлечение данных:** Из таблицы `oz_category_products` извлекаются данные о названиях цветов (`color_name`)
    3. **Анализ расхождений:** Для каждого WB SKU проверяется, есть ли разные значения `color_name` среди связанных товаров
    4. **Статистика:** Подсчитывается количество WB SKU с расхождениями и без них
    5. **Детализация:** Предоставляется подробная информация о каждом найденном расхождении
    
    **Польза анализа:**
    - Выявление inconsistent данных в карточках товаров
    - Улучшение качества данных для лучшей конверсии
    - Стандартизация названий цветов в рамках одного товарного ряда
    - Поиск потенциальных ошибок в заполнении карточек
    """)

    # Массовая стандартизация названий цветов
    st.divider()
    st.header("🔧 Инструмент массовой стандартизации названий цветов")
    st.info("""
    **Цель инструмента:** Автоматически генерировать стандартизированные названия цветов для товаров с расхождениями.
    
    **Принцип работы:**
    - Берется минимальное значение `sort` из `punta_table` для каждого WB SKU
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
    def generate_standardized_color_names(_db_connection, discrepancies_df, details_df):
        """
        Генерирует стандартизированные названия цветов на основе punta_table.sort
        
        Args:
            _db_connection: Соединение с базой данных
            discrepancies_df: DataFrame с расхождениями
            details_df: DataFrame с деталями товаров
            
        Returns:
            DataFrame с результатами стандартизации
        """
        if discrepancies_df.empty or details_df.empty:
            return pd.DataFrame()
        
        try:
            # Получаем все WB SKU с расхождениями
            wb_skus_with_discrepancies = discrepancies_df['wb_sku'].unique().tolist()
            
            if not wb_skus_with_discrepancies:
                return pd.DataFrame()
            
            # Запрос для получения данных из punta_table с sort и позициями
            punta_query = f"""
            WITH wb_sku_with_max_sort AS (
                -- Сначала находим максимальный sort для каждого wb_sku из расхождений
                SELECT 
                    wb_sku,
                    MAX(sort) as max_sort
                FROM punta_table 
                WHERE CAST(wb_sku AS VARCHAR) IN ({', '.join(['?'] * len(wb_skus_with_discrepancies))})
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
                -- Соединяем wb_sku из расхождений с их позициями в полных группах sort
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
            
            punta_df = _db_connection.execute(punta_query, wb_skus_with_discrepancies).fetchdf()
            
            if punta_df.empty:
                st.warning("⚠️ После обработки не найдено валидных данных sort")
                return pd.DataFrame()
            
            # Создаем маппинг wb_sku -> стандартизированный идентификатор
            wb_sku_to_standard_id = {}
            for _, row in punta_df.iterrows():
                # Приводим wb_sku к строке и убираем .0 если есть (для совпадения с данными расхождений)
                wb_sku = str(row['wb_sku']).split('.')[0]
                sort_val = int(row['sort'])
                position = int(row['position_in_sort'])
                standard_id = f"{sort_val}-{position}"
                wb_sku_to_standard_id[wb_sku] = standard_id
            
            # Обрабатываем товары с расхождениями
            standardization_results = []
            
            for wb_sku in wb_skus_with_discrepancies:
                wb_sku_str = str(wb_sku)
                wb_sku_details = details_df[details_df['wb_sku'] == wb_sku_str]
                
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

    # Проверяем, есть ли данные для стандартизации
    if (st.session_state.color_analysis_completed and 
        not st.session_state.color_analysis_discrepancies_df.empty):
        
        st.subheader("🎯 Генерация стандартизированных названий цветов")
        
        discrepancies_df = st.session_state.color_analysis_discrepancies_df
        details_df = st.session_state.color_analysis_details_df
        
        st.write(f"**Найдено {len(discrepancies_df)} WB SKU с расхождениями в названиях цветов.**")
        st.write("Нажмите кнопку ниже для генерации стандартизированных названий.")
        
        col1, col2 = st.columns([1, 3])
        
        with col1:
            if st.button("🔧 Генерировать стандартизированные названия", type="primary", key="generate_standardized_names"):
                with st.spinner("Генерируем стандартизированные названия цветов..."):
                    standardization_results = generate_standardized_color_names(
                        db_connection, discrepancies_df, details_df
                    )
                    
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
        st.info("💡 Для использования инструмента стандартизации сначала выполните анализ расхождений и убедитесь, что найдены товары с расхождениями в названиях цветов.")

            # Кнопка для генерации стандартизированных названий
        if st.button("🎯 Запустить стандартизацию цветов", 
                   key="run_standardization", 
                   use_container_width=True):
            
            with st.spinner('🔄 Генерируем стандартизированные названия цветов...'):
                standardization_df = generate_standardized_color_names(
                    db_connection, 
                    st.session_state.color_discrepancy_results, 
                    st.session_state.color_discrepancy_details
                )

# Close database connection
db_connection.close() 