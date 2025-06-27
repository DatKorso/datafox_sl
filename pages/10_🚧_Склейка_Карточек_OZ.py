"""
Streamlit page for Cards Matcher - товарные карточки.

Эта страница предоставляет функционал для:
- Загрузки рейтингов товаров Ozon
- Анализа и группировки товаров по общим параметрам
- Рекомендаций по объединению/разделению товарных карточек

Updated: Использует UI компоненты для сокращения дублирования кода.
"""
import streamlit as st
import pandas as pd
import os
from utils.db_connection import connect_db
from utils.config_utils import get_data_filter
from utils.db_crud import import_data_from_dataframe
from utils.db_schema import get_table_schema_definition
from utils.cards_matcher_ui_components import (
    render_brand_filter_info,
    render_file_selector_component,
    render_statistics_metrics,
    render_import_process_info,
    render_file_requirements_info,
    render_quick_data_preview,
    render_export_controls,
    render_error_message,
    render_success_message
)
import io
from datetime import datetime
import time
import threading

st.set_page_config(page_title="Cards Matcher - Marketplace Analyzer", layout="wide")
st.title("🃏 Cards Matcher - Управление товарными карточками")
st.markdown("---")

# --- Database Connection ---
conn = connect_db()
if not conn:
    st.error("❌ База данных не подключена. Пожалуйста, настройте подключение в настройках.")
    if st.button("Go to Settings"):
        st.switch_page("pages/3_Settings.py")
    st.stop()

# --- Introduction ---
st.markdown("""
### 🎯 Назначение модуля
Cards Matcher помогает оптимизировать товарные карточки на маркетплейсах:

**Группировка товаров**: Объединение похожих товаров (например, кроссовки одной серии разных цветов) для удобства покупателей и повышения конверсии.

**Разделение проблемных товаров**: Выделение товаров с низким рейтингом из общих групп для сохранения репутации остальных товаров.

**Анализ рейтингов**: Мониторинг рейтингов и отзывов для принятия решений о группировке.
""")

# --- Tabs ---
tab1, tab2, tab3, tab4 = st.tabs(["📊 Загрузка рейтингов Ozon", "🔗 Группировка товаров (в разработке)", "🆕 Расширенная группировка", "✏️ Редактирование существующих групп"])

with tab1:
    st.header("📊 Загрузка рейтингов товаров Ozon")
    
    # Используем компонент для отображения требований к файлу
    render_file_requirements_info()
    
    # --- Brand Filter Information ---
    st.markdown("---")
    st.subheader("🎯 Настройки фильтрации брендов")
    
    brands_filter = get_data_filter("oz_category_products_brands")
    # Используем компонент для отображения информации о фильтре брендов
    render_brand_filter_info(brands_filter)
    
    # --- File Selection ---
    # Используем универсальный компонент выбора файла
    selected_file_path = render_file_selector_component(
        config_key="oz_card_rating_xlsx",
        file_description="с рейтингами Ozon",
        file_types=['xlsx'],
        key_prefix="rating",
        help_text="с рейтингами товаров"
    )
    
    # --- Import Section ---
    if selected_file_path:
        st.markdown("---")
        st.subheader("📥 Импорт рейтингов")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.info("📁 **Файл готов к импорту**")
            st.caption(f"Путь к файлу: {selected_file_path}")
            st.warning("⚠️ **Внимание**: Предыдущие данные рейтингов будут удалены")
        
        with col2:
            if st.button("🚀 Загрузить и импортировать рейтинги", type="primary", key="import_ratings"):
                with st.spinner("Загрузка и импорт данных... Пожалуйста, подождите..."):
                    try:
                        # Read Excel file
                        schema_def = get_table_schema_definition("oz_card_rating")
                        read_params = schema_def.get("read_params", {})
                        
                        st.info("📖 Чтение файла...")
                        import_df = pd.read_excel(selected_file_path, **read_params)
                        
                        if not import_df.empty:
                            # Show file statistics
                            st.success(f"✅ Файл успешно прочитан: {len(import_df)} записей")
                            
                            # Quick validation
                            required_cols = ['RezonitemID', 'Артикул', 'Рейтинг (1)', 'Кол-во отзывов']
                            missing_cols = [col for col in required_cols if col not in import_df.columns]
                            
                            if missing_cols:
                                st.error(f"❌ Отсутствуют обязательные колонки: {missing_cols}")
                            else:
                                # Подготавливаем статистику для компонента
                                stats = {
                                    'total_records': len(import_df),
                                    'unique_skus': len(import_df.get('RezonitemID', pd.Series()).dropna().unique()),
                                    'avg_rating': import_df.get('Рейтинг (1)', pd.Series()).mean()
                                }
                                
                                # Используем компонент для отображения статистики
                                render_statistics_metrics(stats, "📊 Статистика файла")
                                
                                # Migrate schema if needed
                                st.info("🔧 Проверка схемы таблицы...")
                                from utils.db_crud import apply_brand_filter_for_rating, migrate_oz_card_rating_schema
                                migrate_oz_card_rating_schema(conn)
                                
                                # Apply brand filter if configured
                                st.info("🎯 Применение фильтра брендов...")
                                import_df = apply_brand_filter_for_rating(import_df)
                                
                                if import_df.empty:
                                    st.warning("⚠️ После применения фильтра брендов не осталось данных для импорта")
                                    st.info("💡 Проверьте настройки фильтра брендов или содержимое файла")
                                else:
                                    # Import data using existing infrastructure
                                    st.info("💾 Импорт данных в базу...")
                                    success, count, error_message = import_data_from_dataframe(
                                        con=conn,
                                        df=import_df,
                                        table_name="oz_card_rating"
                                    )
                                    
                                    if success:
                                        # Используем компонент для отображения успеха
                                        render_success_message(
                                            "Рейтинги успешно импортированы!",
                                            {"📊 Импортировано записей": count}
                                        )
                                        
                                        # Используем компонент для предпросмотра данных
                                        render_quick_data_preview(import_df, "👀 Предпросмотр импортированных данных")
                                        
                                        # Cleanup temp file if it was uploaded
                                        if selected_file_path.startswith("temp_"):
                                            try:
                                                os.remove(selected_file_path)
                                            except:
                                                pass
                                    else:
                                        render_error_message(Exception(error_message), "при импорте данных")
                        else:
                            st.error("❌ Файл не содержит данных для импорта")
                            
                    except Exception as e:
                        render_error_message(e, "импорта")
                        st.info("Проверьте формат файла и структуру данных")
        
        # Используем компонент для отображения информации о процессе импорта
        render_import_process_info()
    else:
        st.info("👆 **Выберите файл с рейтингами выше для начала импорта**")

with tab2:
    st.header("🔗 Группировка товаров")
    
    st.markdown("""
    ### ✨ Новые возможности группировки
    
    **Ограничение размера группы**: Теперь можно настроить максимальное количество wb_sku в одной группе (по умолчанию 20). 
    Группы, превышающие этот лимит, автоматически разделяются на подгруппы с лучшими товарами в первых подгруппах.
    
    **🎯 Приоритизация по полю sort**: Новая система приоритетов использует поле `sort` из таблицы `punta_table`. 
    Товары с более высоким значением получают приоритет при:
    - Формировании групп (попадают в группы первыми)  
    - Исключении товаров по рейтингу (исключаются в последнюю очередь)
    - Разделении больших групп (попадают в первые подгруппы)
    
    **Преимущества**:
    - 🎯 **Управляемость**: Группы оптимального размера легче управлять
    - 📊 **Анализ**: Более детальная статистика по размерам групп
    - ⚙️ **Гибкость**: Настраиваемое ограничение от 1 до 100 товаров
    - 🔄 **Автоматизация**: Группы разделяются автоматически с сохранением качества
    - 🏆 **Приоритизация**: Умная система приоритетов для оптимальной группировки
    """)
    
    # Check if rating data is available
    try:
        rating_check = conn.execute("SELECT COUNT(*) FROM oz_card_rating").fetchone()
        has_rating_data = rating_check and rating_check[0] > 0
    except:
        has_rating_data = False
    
    if not has_rating_data:
        st.info("📭 **Для работы с группировкой товаров необходимо сначала загрузить данные рейтингов.** Используйте первую вкладку для импорта рейтингов Ozon.")
        
        st.markdown("""
        ### Планируемый функционал после загрузки данных:
        
        #### 🔄 Создание групп товаров
        - Ввод списка артикулов WB (wb_sku)
        - Автоматический поиск связанных товаров Ozon через штрихкоды
        - Вычисление среднего рейтинга wb_sku на основе всех связанных oz_sku
        - Группировка по параметрам из punta_table (модель, категория и др.)
        
        #### 📊 Анализ и рекомендации
        - Рекомендации по объединению товаров в группы
        - Выявление товаров с низким рейтингом для исключения
        - Статистика по группам: размер, рейтинги, количество отзывов
        - Качественная оценка созданных групп
        """)
    else:
        # Import new helper functions
        from utils.cards_matcher_helpers import (
            get_punta_table_data_for_wb_skus,
            create_product_groups, get_available_grouping_columns, analyze_group_quality,
            test_wb_sku_connections
        )
        from utils.cross_marketplace_linker import CrossMarketplaceLinker
        
        st.markdown("### 📝 Ввод данных для группировки")
        
        # Input section for WB SKUs
        col1, col2 = st.columns([2, 1])
        
        with col1:
            wb_skus_input = st.text_area(
                "**Введите артикулы WB (wb_sku):**",
                height=150,
                help="Введите wb_sku через пробел, запятую или каждый на новой строке.\nПример: 123456 789012 или\n123456\n789012",
                placeholder="123456 789012\n345678\n901234"
            )
        
        with col2:
            st.info("""
            **💡 Как это работает:**
            
            1. Вводите wb_sku
            2. Система находит связанные oz_sku через штрихкоды
            3. Вычисляет средний рейтинг wb_sku
            4. Группирует по выбранным параметрам
            5. Показывает рекомендации
            """)
        
        # Parse WB SKUs from input
        wb_skus = []
        if wb_skus_input.strip():
            # Parse input - support spaces, commas, and newlines
            import re
            wb_skus_raw = re.split(r'[,\s\n]+', wb_skus_input.strip())
            wb_skus = [sku.strip() for sku in wb_skus_raw if sku.strip().isdigit()]
        
        if wb_skus:
            st.success(f"✅ Распознано {len(wb_skus)} валидных wb_sku")
            
            # Show preview of parsed SKUs
            with st.expander("📋 Просмотр распознанных wb_sku"):
                st.write(", ".join(wb_skus[:20]) + ("..." if len(wb_skus) > 20 else ""))
        
        # Grouping configuration
        st.markdown("---")
        st.markdown("### ⚙️ Настройки группировки")
        
        # Get available columns from punta_table
        available_columns = get_available_grouping_columns(conn) if wb_skus else []
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            min_group_rating = st.slider(
                "Минимальный рейтинг группы", 
                0.0, 5.0, 3.0, 0.1,
                help="Из групп будут исключены wb_sku с низким рейтингом, чтобы минимальный рейтинг группы соответствовал этому значению"
            )
        
        with col2:
            max_wb_sku_per_group = st.number_input(
                "Максимум wb_sku в группе",
                min_value=1,
                max_value=100,
                value=20,
                step=1,
                help="Группы, превышающие этот размер, будут автоматически разделены на подгруппы"
            )
        
        with col3:
            enable_sort_priority = st.checkbox(
                "🎯 Приоритизация по полю sort",
                value=True,
                help="Товары с более высоким значением 'sort' из punta_table получают приоритет при формировании групп"
            )
        
        with col4:
            # Grouping columns selection
            if available_columns:
                st.info(f"📋 Найдено {len(available_columns)} колонок в punta_table")
                
                grouping_columns = st.multiselect(
                    "Колонки для группировки из punta_table:",
                    options=available_columns,
                    default=available_columns[:2] if len(available_columns) >= 2 else available_columns,
                    help="Выберите колонки по которым товары будут объединяться в группы"
                )
            else:
                grouping_columns = []
                if wb_skus:
                    st.warning("⚠️ Таблица punta_table не найдена или пуста. Группировка будет только по рейтингам.")
        
        # Analysis and grouping
        if wb_skus:
            st.markdown("---")
            
            # Отладочная секция для тестирования связанных товаров
            with st.expander("🔍 Тест поиска связанных товаров", expanded=False):
                st.info("Используйте эту секцию для тестирования поиска связанных товаров для отдельных wb_sku")
                
                test_wb_sku = st.text_input(
                    "Тестовый wb_sku:",
                    value="191826729",
                    help="Введите wb_sku для тестирования поиска связанных oz_sku"
                )
                
                if st.button("🔍 Найти связанные товары", key="test_linked"):
                    if test_wb_sku.strip():
                        with st.spinner("Детальный анализ связанных товаров..."):
                            from utils.cards_matcher_helpers import test_wb_sku_connections
                            
                            # Детальное тестирование с отладочной информацией
                            test_results = test_wb_sku_connections(conn, test_wb_sku.strip(), show_debug=True)
                            
                            # Показываем отладочную информацию
                            st.subheader("📊 Результаты анализа")
                            
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Штрихкодов WB", test_results['wb_barcodes_count'])
                            with col2:
                                st.metric("Связанных записей", test_results['linked_products_count'])
                            with col3:
                                st.metric("Уникальных OZ SKU", test_results['unique_oz_sku_count'])
                            with col4:
                                st.metric("С рейтингами", test_results['rated_oz_sku_count'])
                            
                            # Показываем пошаговую отладочную информацию
                            st.subheader("🔍 Пошаговая диагностика")
                            for info in test_results['debug_info']:
                                if info.startswith("✅"):
                                    st.success(info)
                                elif info.startswith("❌"):
                                    st.error(info)
                                else:
                                    st.info(info)
                            
                            # Финальные результаты
                            if test_results['final_rating_data'] is not None and not test_results['final_rating_data'].empty:
                                st.subheader("📋 Итоговые данные")
                                st.dataframe(test_results['final_rating_data'], use_container_width=True)
                                
                                # Детальная информация
                                row = test_results['final_rating_data'].iloc[0]
                                st.info(f"""
                                **WB SKU:** {row['wb_sku']}  
                                **Связанные OZ SKU:** {row['oz_skus_list']}  
                                **Количество OZ SKU:** {row['oz_sku_count']}  
                                **Средний рейтинг:** {row['avg_rating']}  
                                **Диапазон рейтингов:** {row['min_rating']} - {row['max_rating']}  
                                **Общее количество отзывов:** {row['total_reviews']}
                                """)
                            else:
                                st.warning("❌ Итоговые данные не сформированы")
                    else:
                        st.warning("Введите wb_sku для тестирования")
            
            if st.button("🚀 Создать группы товаров", type="primary", key="create_groups"):
                with st.spinner("Анализ товаров и создание групп..."):
                    
                    # Step 1: Get WB SKU ratings using new centralized linker
                    linker = CrossMarketplaceLinker(conn)
                    wb_ratings_df = linker.get_links_with_ozon_ratings(wb_skus)
                    
                    if wb_ratings_df.empty:
                        st.error("❌ Не найдены связанные товары Ozon для указанных wb_sku")
                        st.info("💡 Убедитесь, что:")
                        st.info("• Указанные wb_sku существуют в базе данных")
                        st.info("• У товаров есть общие штрихкоды с товарами Ozon")
                        st.info("• Данные рейтингов загружены для связанных oz_sku")
                    else:
                        st.success(f"✅ Найдены рейтинги для {len(wb_ratings_df)} wb_sku")
                        
                        # Step 2: Create groups
                        groups_df = create_product_groups(
                            conn=conn,
                            wb_skus=wb_skus,
                            grouping_columns=grouping_columns,
                            min_group_rating=min_group_rating,
                            max_wb_sku_per_group=max_wb_sku_per_group,
                            enable_sort_priority=enable_sort_priority
                        )
                        
                        if groups_df.empty:
                            st.warning("⚠️ Не удалось создать группы с текущими настройками")
                            st.info("💡 Попробуйте:")
                            st.info("• Снизить минимальный рейтинг")
                            st.info("• Изменить настройки исключения низкорейтинговых товаров")
                            st.info("• Выбрать другие колонки для группировки")
                        else:
                            # Step 3: Show results
                            st.markdown("### 📊 Результаты группировки")
                            
                            # Quality analysis
                            quality_metrics = analyze_group_quality(groups_df)
                            
                            if quality_metrics:
                                # Show summary metrics
                                col1, col2, col3, col4 = st.columns(4)
                                
                                with col1:
                                    st.metric("🔗 Всего групп", quality_metrics['total_groups'])
                                
                                with col2:
                                    st.metric("📦 Товаров обработано", quality_metrics['total_products'])
                                
                                with col3:
                                    st.metric("⭐ Ср. рейтинг групп", quality_metrics['avg_group_rating'])
                                
                                with col4:
                                    st.metric("🏆 Отличных групп", quality_metrics['excellent_groups_count'])
                                
                                # Show additional metrics about group sizes
                                if quality_metrics.get('max_group_size', 0) > 0:
                                    # Проверяем, есть ли данные приоритизации
                                    priority_stats = quality_metrics.get('priority_stats', {})
                                    has_priority = priority_stats.get('has_priority_data', False)
                                    
                                    if has_priority:
                                        # 5 колонок если есть приоритизация
                                        col1, col2, col3, col4, col5 = st.columns(5)
                                        
                                        with col5:
                                            st.metric("🎯 Ср. приоритет", priority_stats['avg_sort_value'])
                                    else:
                                        # 4 колонки если нет приоритизации
                                        col1, col2, col3, col4 = st.columns(4)
                                    
                                    with col1:
                                        st.metric("📏 Средний размер группы", quality_metrics['avg_group_size'])
                                    
                                    with col2:
                                        st.metric("📊 Макс. размер группы", quality_metrics['max_group_size'])
                                    
                                    with col3:
                                        split_count = quality_metrics.get('split_groups_count', 0)
                                        st.metric("✂️ Разделенных групп", split_count)
                                    
                                    with col4:
                                        large_count = quality_metrics.get('large_groups_count', 0)
                                        st.metric("📈 Больших групп", large_count)
                                    
                                    # Дополнительная информация о приоритизации
                                    if has_priority:
                                        st.markdown("### 🎯 Информация о приоритизации")
                                        
                                        col1, col2, col3 = st.columns(3)
                                        
                                        with col1:
                                            st.metric("📊 Товаров с приоритетом", priority_stats['products_with_priority'])
                                        
                                        with col2:
                                            st.metric("🔝 Макс. приоритет", priority_stats['max_sort_value'])
                                        
                                        with col3:
                                            st.metric("⬆️ Высокоприоритетных", priority_stats['high_priority_products'])
                                
                                # Show detailed results table
                                st.markdown("### 📋 Детальная информация по группам")
                                
                                # Prepare display columns
                                display_columns = [
                                    'group_id', 'wb_sku', 'avg_rating', 'oz_sku_count', 
                                    'oz_skus_list', 'total_reviews', 'wb_count', 
                                    'group_avg_rating', 'total_oz_sku_count', 'group_total_reviews',
                                    'group_recommendation'
                                ]
                                
                                # Add sort column if priority is enabled and column exists
                                if enable_sort_priority and 'sort' in groups_df.columns:
                                    display_columns.insert(2, 'sort')  # Insert after wb_sku
                                
                                # Add grouping columns if they exist
                                if grouping_columns:
                                    for col in grouping_columns:
                                        if col in groups_df.columns and col not in display_columns:
                                            display_columns.insert(-1, col)  # Insert before recommendation
                                
                                # Filter available columns
                                available_display_columns = [col for col in display_columns if col in groups_df.columns]
                                
                                # Configure column names for display
                                column_config = {
                                    'group_id': st.column_config.NumberColumn('Группа №', width="small"),
                                    'wb_sku': 'WB SKU',
                                    'avg_rating': st.column_config.NumberColumn('Рейтинг WB', format="%.2f"),
                                    'oz_sku_count': st.column_config.NumberColumn('Кол-во OZ SKU', width="small"),
                                    'oz_skus_list': st.column_config.TextColumn('OZ SKU список', width="medium"),
                                    'total_reviews': st.column_config.NumberColumn('Отзывов', width="small"),
                                    'wb_count': st.column_config.NumberColumn('WB в группе', width="small"),
                                    'group_avg_rating': st.column_config.NumberColumn('Рейтинг группы', format="%.2f"),
                                    'total_oz_sku_count': st.column_config.NumberColumn('Всего OZ SKU', width="small"),
                                    'group_total_reviews': st.column_config.NumberColumn('Отзывов группы', width="small"),
                                    'group_recommendation': 'Рекомендация'
                                }
                                
                                # Добавляем конфигурацию для колонки sort если она есть
                                if 'sort' in groups_df.columns:
                                    column_config['sort'] = st.column_config.NumberColumn('🎯 Приоритет', format="%.1f", width="small")
                                
                                st.dataframe(
                                    groups_df[available_display_columns],
                                    use_container_width=True,
                                    hide_index=True,
                                    column_config=column_config
                                )
                                
                                # Group analysis by recommendations
                                st.markdown("### 🎯 Анализ рекомендаций")
                                
                                recommendations = groups_df['group_recommendation'].value_counts()
                                
                                for recommendation, count in recommendations.items():
                                    color = "green" if "Отличная" in recommendation else \
                                           "blue" if "Хорошая" in recommendation else \
                                           "orange" if "Удовлетворительная" in recommendation else \
                                           "red" if "Исключить" in recommendation else "gray"
                                    
                                    st.markdown(f":{color}[**{recommendation}**: {count} групп]")
                                
                                # Additional analysis for group sizes
                                if quality_metrics.get('split_groups_count', 0) > 0 or quality_metrics.get('large_groups_count', 0) > 0:
                                    st.markdown("### 📊 Анализ размеров групп")
                                    
                                    size_metrics = []
                                    if quality_metrics.get('single_product_groups', 0) > 0:
                                        size_metrics.append(f"🔸 **Одиночные товары**: {quality_metrics['single_product_groups']}")
                                    if quality_metrics.get('small_groups', 0) > 0:
                                        size_metrics.append(f"🔹 **Малые группы** (2-5 товаров): {quality_metrics['small_groups']}")
                                    if quality_metrics.get('medium_groups', 0) > 0:
                                        size_metrics.append(f"🔷 **Средние группы** (6-15 товаров): {quality_metrics['medium_groups']}")
                                    if quality_metrics.get('large_groups_size_count', 0) > 0:
                                        size_metrics.append(f"🔶 **Большие группы** (16+ товаров): {quality_metrics['large_groups_size_count']}")
                                    
                                    for metric in size_metrics:
                                        st.markdown(metric)
                                    
                                    if quality_metrics.get('split_groups_count', 0) > 0:
                                        st.info(f"ℹ️ **{quality_metrics['split_groups_count']} групп было автоматически разделено** в связи с превышением лимита {max_wb_sku_per_group} товаров в группе")
                                
                                # Export functionality
                                st.markdown("---")
                                col1, col2 = st.columns(2)
                                
                                with col1:
                                    # Используем компонент для экспорта
                                    render_export_controls(groups_df, "product_groups", "результаты группировки")
                                
                                with col2:
                                    st.info("💡 **Совет**: Используйте рекомендации для оптимизации групп товаров на маркетплейсе")
        
        else:
            st.info("👆 **Введите wb_sku выше для начала работы с группировкой товаров**")

with tab3:
    st.header("🆕 Расширенная группировка товаров")
    
    st.markdown("""
    ### 🚀 Новый алгоритм группировки с расширенными возможностями
    
    **Ключевые особенности:**
    - 🏆 **Умная приоритизация**: Товары с максимальным `sort`, затем с остатками
    - ⭐ **Высокий рейтинг = отдельная карточка**: Товары с рейтингом выше минимального порога остаются в отдельных группах
    - 📊 **Компенсация рейтинга**: Автоматический поиск товаров для повышения группового рейтинга (только для товаров с низким рейтингом)
    - 👥 **Обязательная группировка по полу**: Мальчики и девочки всегда в разных группах
    - 🚫 **Обработка брака**: Товары с "БракSH" получают уникальные коды
    - 📋 **Детальная отчетность**: Разделение на успешные группы, товары без связей и с низким рейтингом
    
    **Алгоритм работы:**
    1. Анализ входного списка wb_sku по категориям
    2. Поиск связанных товаров Ozon и их рейтингов
    3. Суммирование остатков по wb_sku
    4. Приоритизация: максимальный sort → товары с остатками → остальные
    5. **ВАЖНО**: Товары с рейтингом ≥ минимального порога → отдельные группы
    6. Группировка товаров с низким рейтингом по выбранным критериям
    7. Компенсация низкого рейтинга аналогичными товарами
    8. Создание финальных групп с уникальными merge_on_card
    
    **💡 Принцип "Высокий рейтинг = отдельная карточка":**
    - Если товар уже имеет рейтинг выше указанного минимума, он остается в отдельной группе
    - Такие товары получают собственный wb_sku как код объединения (merge_on_card)
    - Это гарантирует, что качественные товары не "разбавляются" менее качественными
    """)
    
    # Check if rating data is available
    try:
        rating_check = conn.execute("SELECT COUNT(*) FROM oz_card_rating").fetchone()
        has_rating_data = rating_check and rating_check[0] > 0
    except:
        has_rating_data = False
    
    if not has_rating_data:
        st.warning("📭 **Для работы расширенной группировки необходимо сначала загрузить данные рейтингов.** Используйте первую вкладку для импорта рейтингов Ozon.")
    else:
        # Import the new advanced helper function
        from utils.cards_matcher_helpers import (
            create_advanced_product_groups,
            get_available_grouping_columns
        )
        
        st.markdown("---")
        st.markdown("### 📝 Настройки расширенной группировки")
        
        # Input section for WB SKUs
        col1, col2 = st.columns([2, 1])
        
        with col1:
            wb_skus_input_advanced = st.text_area(
                "**Введите артикулы WB (wb_sku):**",
                height=120,
                help="Введите wb_sku через пробел, запятую или каждый на новой строке",
                placeholder="123456 789012\n345678\n901234",
                key="advanced_wb_skus"
            )
            
        with col2:
            st.info("""
            **🎯 Новый алгоритм:**
            
            1. Анализ приоритетов
            2. Группировка по gender+
            3. Компенсация рейтинга
            4. Обработка брака
            5. Детальные отчеты
            """)
        
        # Parse WB SKUs from input
        wb_skus_advanced = []
        if wb_skus_input_advanced.strip():
            import re
            wb_skus_raw = re.split(r'[,\s\n]+', wb_skus_input_advanced.strip())
            wb_skus_advanced = [sku.strip() for sku in wb_skus_raw if sku.strip().isdigit()]
        
        if wb_skus_advanced:
            st.success(f"✅ Распознано {len(wb_skus_advanced)} валидных wb_sku")
            
            # Show preview of parsed SKUs
            with st.expander("📋 Просмотр распознанных wb_sku"):
                st.write(", ".join(wb_skus_advanced[:20]) + ("..." if len(wb_skus_advanced) > 20 else ""))
        
        # Advanced configuration
        st.markdown("---")
        st.markdown("### ⚙️ Расширенные настройки")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            min_group_rating_advanced = st.slider(
                "Минимальный рейтинг группы", 
                0.0, 5.0, 4.0, 0.1,
                help="Группы с рейтингом ниже этого значения будут дополнены компенсирующими товарами",
                key="advanced_min_rating"
            )
        
        with col2:
            max_wb_sku_per_group_advanced = st.number_input(
                "Максимум wb_sku в группе",
                min_value=1,
                max_value=50,
                value=15,
                step=1,
                help="Ограничение размера группы для удобства управления",
                key="advanced_max_group"
            )
        
        with col3:
            enable_sort_priority_advanced = st.checkbox(
                "🎯 Приоритизация по sort",
                value=True,
                help="Товары с максимальным значением sort получают приоритет",
                key="advanced_sort_priority"
            )
        
        with col4:
            # Category filter
            wb_category_filter = st.text_input(
                "Фильтр по категории WB",
                placeholder="Например: Обувь",
                help="Оставьте пустым для обработки всех категорий",
                key="advanced_category_filter"
            )
        
        # Important notice about high rating logic
        st.info(f"""
        **🎯 Важно понимать:** Товары с рейтингом **≥ {min_group_rating_advanced}** будут помещены в **отдельные группы**.
        
        - Такие товары получат собственный wb_sku как код объединения (merge_on_card)
        - Они НЕ будут объединяться с другими товарами
        - Только товары с рейтингом **< {min_group_rating_advanced}** будут группироваться и получать компенсацию
        """)
        
        # Grouping columns selection
        st.markdown("#### 🔗 Поля для группировки")
        
        if wb_skus_advanced:
            available_columns = get_available_grouping_columns(conn)
            
            if available_columns:
                # Remove gender from options since it's mandatory
                grouping_options = [col for col in available_columns if col != 'gender']
                
                st.info("🔒 **Поле 'gender' включено автоматически** (обязательное для разделения мальчики/девочки)")
                
                grouping_columns_advanced = st.multiselect(
                    "Дополнительные поля для группировки:",
                    options=grouping_options,
                    default=['season', 'material'] if all(col in grouping_options for col in ['season', 'material']) else grouping_options[:2],
                    help="Товары будут группироваться по gender + выбранным полям",
                    key="advanced_grouping_cols"
                )
            else:
                grouping_columns_advanced = []
                st.warning("⚠️ Таблица punta_table не найдена или пуста")
        else:
            grouping_columns_advanced = []
        
        # Advanced grouping execution
        if wb_skus_advanced:
            st.markdown("---")
            
            # Run advanced grouping button
            col1, col2 = st.columns([2, 1])
            
            with col1:
                run_grouping = st.button("🚀 Запустить расширенную группировку", type="primary", key="run_advanced_grouping")
            
            with col2:
                if wb_skus_advanced and len(wb_skus_advanced) > 100:
                    st.info("⏳ **Большой объем данных**\n\nДля обработки >100 товаров будет показан детальный прогресс-бар с пошаговым статусом операций.")
            
            if run_grouping:
                if not wb_skus_advanced:
                    st.error("⚠️ Введите список wb_sku для группировки")
                else:
                    # Показываем оценку времени выполнения
                    wb_count = len(wb_skus_advanced)
                    estimated_time = wb_count * 0.5  # Примерно 0.5 сек на товар
                    
                    if wb_count > 50:
                        st.info(f"📊 Будет обработано {wb_count} товаров. Ориентировочное время: {estimated_time:.0f}-{estimated_time*2:.0f} секунд")
                    
                    with st.spinner("Подготовка к группировке..."):
                        time.sleep(0.5)  # Небольшая задержка для показа спиннера
                    
                    try:
                        # Выполняем расширенную группировку с прогресс-баром
                        start_time = time.time()
                        progress_bar = st.progress(0)
                        status_placeholder = st.empty()
                        
                        def update_progress(progress: float, status: str):
                            """Callback функция для обновления прогресса"""
                            elapsed = time.time() - start_time
                            progress_bar.progress(progress)
                            status_placeholder.info(f"⏳ {status} (⏱️ {elapsed:.1f}с)")
                        
                        # Запускаем группировку с колбэком прогресса
                        groups_df, no_links_df, low_rating_df = create_advanced_product_groups(
                            conn,
                            wb_skus_advanced,
                            grouping_columns_advanced,
                            min_group_rating_advanced,
                            max_wb_sku_per_group_advanced,
                            enable_sort_priority_advanced,
                            wb_category_filter.strip() if wb_category_filter else None,
                            progress_callback=update_progress
                        )
                        
                        # Показываем общее время выполнения
                        total_time = time.time() - start_time
                        
                        # Очищаем прогресс-бар после завершения
                        progress_bar.empty()
                        status_placeholder.success(f"✅ Группировка завершена за {total_time:.1f} секунд")
                        
                        # Автоматически скрываем статус через 3 секунды
                        def clear_status():
                            """Очищает статус через 3 секунды"""
                            import time
                            import streamlit as st
                            time.sleep(3)
                            try:
                                # Проверяем, что контекст Streamlit еще активен
                                if hasattr(st, '_get_session_state') and st.session_state:
                                    status_placeholder.empty()
                                    progress_placeholder.empty()
                            except Exception as e:
                                # Игнорируем ошибки при очистке UI
                                pass
                        
                        threading.Thread(target=clear_status, daemon=True).start()
                        
                        # Сохраняем результаты в session_state для экспорта
                        st.session_state['advanced_groups_df'] = groups_df
                        st.session_state['advanced_no_links_df'] = no_links_df
                        st.session_state['advanced_low_rating_df'] = low_rating_df
                        st.session_state['advanced_results_timestamp'] = datetime.now().strftime('%Y%m%d_%H%M')
                        st.session_state['advanced_results_ready'] = True
                        st.session_state['advanced_wb_skus_count'] = len(wb_skus_advanced)
                        st.session_state['advanced_min_group_rating'] = min_group_rating_advanced
                        # ВАЖНО: Сохраняем входные wb_sku для правильного подсчета статистики
                        st.session_state['advanced_input_wb_skus'] = wb_skus_advanced.copy()
                    
                    except Exception as e:
                        render_error_message(e, "расширенной группировки")
                        st.info("Проверьте правильность входных данных и настроек")
        
        # Отображение результатов из session_state (независимо от обработки)
        if st.session_state.get('advanced_results_ready', False):
            # Получаем сохраненные результаты
            saved_groups_df = st.session_state.get('advanced_groups_df', pd.DataFrame())
            saved_no_links_df = st.session_state.get('advanced_no_links_df', pd.DataFrame())
            saved_low_rating_df = st.session_state.get('advanced_low_rating_df', pd.DataFrame())
            saved_wb_skus_count = st.session_state.get('advanced_wb_skus_count', 0)
            saved_min_group_rating = st.session_state.get('advanced_min_group_rating', 0.0)
            timestamp = st.session_state.get('advanced_results_timestamp', datetime.now().strftime('%Y%m%d_%H%M'))
            
            # Показываем результаты
            st.markdown("### 📊 Результаты расширенной группировки")
            
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("📦 Всего обработано", saved_wb_skus_count)
            
            with col2:
                successful_groups = len(saved_groups_df['group_id'].unique()) if not saved_groups_df.empty else 0
                st.metric("✅ Успешных групп", successful_groups)
            
            with col3:
                no_links_count = len(saved_no_links_df) if not saved_no_links_df.empty else 0
                st.metric("❌ Без связей", no_links_count)
            
            with col4:
                low_rating_count = len(saved_low_rating_df) if not saved_low_rating_df.empty else 0
                st.metric("⭐ Низкий рейтинг", low_rating_count)
            
            # Show successful groups
            if not saved_groups_df.empty:
                st.success(f"✅ Создано {len(saved_groups_df)} групп из {saved_groups_df['group_id'].nunique()} уникальных групп")
                
                # Calculate statistics
                total_wb_count = saved_groups_df['wb_count'].sum()
                high_rating_groups = saved_groups_df[saved_groups_df['group_recommendation'] == 'Высокий рейтинг - отдельная карточка']['group_id'].nunique()
                compensated_groups = saved_groups_df[saved_groups_df['group_recommendation'] != 'Высокий рейтинг - отдельная карточка']['group_id'].nunique()
                defective_groups = saved_groups_df[saved_groups_df['is_defective'] == True]['group_id'].nunique()
                
                # Enhanced metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Общий охват", f"{total_wb_count} wb_sku")
                with col2:
                    st.metric("🌟 Высокий рейтинг", f"{high_rating_groups} групп", 
                            help="Отдельные группы для товаров с высоким рейтингом")
                with col3:
                    st.metric("🔧 Компенсированные", f"{compensated_groups} групп",
                            help="Группы с компенсацией низкого рейтинга")
                with col4:
                    if defective_groups > 0:
                        st.metric("🚫 Брак", f"{defective_groups} групп")
                    else:
                        st.metric("Средний рейтинг", f"{saved_groups_df['group_avg_rating'].mean():.2f}")
                
                # Separate high rating and compensated groups for clearer display
                high_rating_df = saved_groups_df[saved_groups_df['group_recommendation'] == 'Высокий рейтинг - отдельная карточка']
                other_groups_df = saved_groups_df[saved_groups_df['group_recommendation'] != 'Высокий рейтинг - отдельная карточка']
                
                # Display high rating groups separately
                if not high_rating_df.empty:
                    st.markdown("#### 🌟 Товары с высоким рейтингом (отдельные группы)")
                    st.info(f"Эти {len(high_rating_df)} товаров остались в отдельных группах так как их рейтинг ≥ {saved_min_group_rating}")
                    
                    # Show as separate groups
                    display_columns = ['wb_sku']
                    if 'oz_vendor_code' in high_rating_df.columns:
                        display_columns.append('oz_vendor_code')
                    display_columns.extend(['merge_on_card', 'avg_rating', 'gender'])
                    
                    high_rating_display = high_rating_df[display_columns].copy()
                    column_names = ['WB SKU']
                    if 'oz_vendor_code' in high_rating_df.columns:
                        column_names.append('Артикул Ozon')
                    column_names.extend(['Код объединения', 'Рейтинг', 'Пол'])
                    
                    high_rating_display.columns = column_names
                    
                    st.dataframe(
                        high_rating_display,
                        use_container_width=True,
                        height=min(200, len(high_rating_display) * 35 + 50)
                    )
                
                # Display compensated/other groups
                if not other_groups_df.empty:
                    st.markdown("#### 🔧 Компенсированные и специальные группы")
                    
                    # Group by merge_on_card and show group statistics
                    group_stats = []
                    for merge_code, group_data in other_groups_df.groupby('merge_on_card'):
                        group_stats.append({
                            'Код объединения': merge_code,
                            'Количество товаров': len(group_data),
                            'Рейтинг группы': group_data['group_avg_rating'].iloc[0],
                            'Тип': group_data['group_recommendation'].iloc[0],
                            'Пол': group_data['gender'].iloc[0] if 'gender' in group_data.columns else 'Не указан'
                        })
                    
                    group_stats_df = pd.DataFrame(group_stats)
                    st.dataframe(
                        group_stats_df,
                        use_container_width=True,
                        height=min(300, len(group_stats_df) * 35 + 50)
                    )
                    
                    # Detailed view option
                    if st.checkbox("📋 Показать детальный состав групп", key="show_detailed_advanced"):
                        for merge_code, group_data in other_groups_df.groupby('merge_on_card'):
                            with st.expander(f"Группа {merge_code} ({len(group_data)} товаров, рейтинг: {group_data['group_avg_rating'].iloc[0]:.2f})"):
                                # Включаем oz_vendor_code если доступно (на второй позиции)
                                detail_columns = ['wb_sku']
                                if 'oz_vendor_code' in group_data.columns:
                                    detail_columns.append('oz_vendor_code')
                                detail_columns.extend(['avg_rating', 'gender'])
                                
                                detailed_data = group_data[detail_columns].copy()
                                column_names = ['WB SKU']
                                if 'oz_vendor_code' in group_data.columns:
                                    column_names.append('Артикул Ozon')
                                column_names.extend(['Индивидуальный рейтинг', 'Пол'])
                                
                                detailed_data.columns = column_names
                                st.dataframe(detailed_data, use_container_width=True)
                
                # Export functionality with clear separation
                st.markdown("#### 📁 Экспорт результатов")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if st.button("📊 Экспорт всех групп (Excel)", key="export_all_excel_advanced"):
                        try:
                            excel_buffer = io.BytesIO()
                            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                                if not saved_groups_df.empty:
                                    high_rating_df_export = saved_groups_df[saved_groups_df['group_recommendation'] == 'Высокий рейтинг - отдельная карточка']
                                    other_groups_df_export = saved_groups_df[saved_groups_df['group_recommendation'] != 'Высокий рейтинг - отдельная карточка']
                                    
                                    if not high_rating_df_export.empty:
                                        high_rating_df_export.to_excel(writer, sheet_name='Высокий рейтинг', index=False)
                                    if not other_groups_df_export.empty:
                                        other_groups_df_export.to_excel(writer, sheet_name='Компенсированные', index=False)
                                if not saved_no_links_df.empty:
                                    saved_no_links_df.to_excel(writer, sheet_name='Без связей', index=False)
                                if not saved_low_rating_df.empty:
                                    saved_low_rating_df.to_excel(writer, sheet_name='Низкий рейтинг', index=False)
                            
                            st.download_button(
                                label="⬇️ Скачать Excel",
                                data=excel_buffer.getvalue(),
                                file_name=f"расширенная_группировка_{timestamp}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_excel_advanced"
                            )
                        except Exception as e:
                            st.error(f"Ошибка создания Excel: {e}")
                
                with col2:
                    if st.button("📝 Экспорт всех групп (CSV)", key="export_all_csv_advanced"):
                        try:
                            if not saved_groups_df.empty:
                                csv_data = saved_groups_df.to_csv(index=False).encode('utf-8')
                                st.download_button(
                                    label="⬇️ Скачать CSV",
                                    data=csv_data,
                                    file_name=f"расширенная_группировка_{timestamp}.csv",
                                    mime="text/csv",
                                    key="download_csv_advanced"
                                )
                            else:
                                st.warning("Нет данных для экспорта")
                        except Exception as e:
                            st.error(f"Ошибка создания CSV: {e}")
                
                with col3:
                    # Summary export option
                    if st.button("📊 Экспорт сводки", key="export_summary_advanced"):
                        try:
                            summary_data = []
                            
                            if not saved_groups_df.empty:
                                high_rating_df_summary = saved_groups_df[saved_groups_df['group_recommendation'] == 'Высокий рейтинг - отдельная карточка']
                                other_groups_df_summary = saved_groups_df[saved_groups_df['group_recommendation'] != 'Высокий рейтинг - отдельная карточка']
                                
                                # Add high rating groups summary
                                for _, row in high_rating_df_summary.iterrows():
                                    summary_data.append({
                                        'wb_sku': row['wb_sku'],
                                        'merge_on_card': row['merge_on_card'],
                                        'type': 'Высокий рейтинг',
                                        'group_size': 1,
                                        'group_rating': row['avg_rating']
                                    })
                                
                                # Add other groups summary
                                for merge_code, group_data in other_groups_df_summary.groupby('merge_on_card'):
                                    summary_data.append({
                                        'wb_sku': ', '.join(group_data['wb_sku'].astype(str)),
                                        'merge_on_card': merge_code,
                                        'type': group_data['group_recommendation'].iloc[0],
                                        'group_size': len(group_data),
                                        'group_rating': group_data['group_avg_rating'].iloc[0]
                                    })
                            
                            if summary_data:
                                summary_df = pd.DataFrame(summary_data)
                                csv_data = summary_df.to_csv(index=False).encode('utf-8')
                                
                                st.download_button(
                                    label="⬇️ Скачать сводку",
                                    data=csv_data,
                                    file_name=f"сводка_группировки_{timestamp}.csv",
                                    mime="text/csv",
                                    key="download_summary_advanced"
                                )
                            else:
                                st.warning("Нет данных для создания сводки")
                        except Exception as e:
                            st.error(f"Ошибка создания сводки: {e}")
            
            else:
                st.warning("⚠️ Не удалось создать ни одной группы")
            
            # Show items without links
            if not saved_no_links_df.empty:
                st.markdown("#### ❌ Товары без связанных данных Ozon")
                st.warning(f"Найдено {len(saved_no_links_df)} товаров без связей с товарами Ozon")
                
                st.dataframe(
                    saved_no_links_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        'wb_sku': st.column_config.TextColumn('WB SKU', width="medium"),
                        'issue': st.column_config.TextColumn('Проблема', width="large")
                    }
                )
                
                render_export_controls(saved_no_links_df, "no_links_items", "товары без связей")
            
            # Show items with low rating - ВСЕГДА показываем раздел
            st.markdown("#### ⭐ Товары с низким рейтингом")
            
            if not saved_low_rating_df.empty:
                st.error(f"🚨 Найдено {len(saved_low_rating_df)} товаров, для которых НЕ УДАЛОСЬ компенсировать рейтинг до {saved_min_group_rating}")
                
                st.dataframe(
                    saved_low_rating_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        'wb_sku': st.column_config.TextColumn('WB SKU', width="medium"),
                        'avg_rating': st.column_config.NumberColumn('Рейтинг', format="%.2f", width="small"),
                        'issue': st.column_config.TextColumn('Проблема', width="large")
                    }
                )
                
                render_export_controls(saved_low_rating_df, "low_rating_items", "товары с низким рейтингом")
            else:
                # Показываем статус даже если нет товаров с низким рейтингом
                processed_count = saved_wb_skus_count - (len(saved_no_links_df) if not saved_no_links_df.empty else 0)
                
                if not saved_groups_df.empty:
                    # Правильный подсчет: считаем уникальные группы, а не товары в группах
                    unique_groups = saved_groups_df['merge_on_card'].nunique()
                    high_rating_groups = saved_groups_df[saved_groups_df['group_recommendation'] == 'Высокий рейтинг - отдельная карточка']['merge_on_card'].nunique()
                    compensated_groups = saved_groups_df[saved_groups_df['group_recommendation'] != 'Высокий рейтинг - отдельная карточка']['merge_on_card'].nunique()
                    
                    # ВАЖНО: Считаем только входные wb_sku пользователя, которые успешно попали в группы
                    # Создаем список входных wb_sku из сессии (если есть)
                    input_wb_skus = set()
                    if 'advanced_input_wb_skus' in st.session_state:
                        input_wb_skus = set(st.session_state['advanced_input_wb_skus'])
                    
                    # Если нет в сессии, считаем что все уникальные merge_on_card - это входные товары
                    # (это приблизительная оценка для случаев когда сессия потеряна)
                    if not input_wb_skus:
                        # Пытаемся извлечь из merge_on_card первичные wb_sku
                        primary_wb_skus = set()
                        for merge_code in saved_groups_df['merge_on_card'].unique():
                            # Для обычных групп merge_on_card содержит wb_sku основного товара
                            if not str(merge_code).startswith('БракSH_'):
                                primary_wb_skus.add(str(merge_code))
                        input_wb_skus = primary_wb_skus
                    
                    # Подсчитываем сколько входных товаров успешно попало в группы
                    successfully_grouped_input_items = 0
                    for wb_sku in input_wb_skus:
                        if str(wb_sku) in saved_groups_df['wb_sku'].astype(str).values or \
                           str(wb_sku) in saved_groups_df['merge_on_card'].astype(str).values:
                            successfully_grouped_input_items += 1
                    
                    st.success(f"✅ Все товары успешно обработаны:")
                    st.write(f"• **{unique_groups} групп создано** ({high_rating_groups} с высоким рейтингом + {compensated_groups} со скомпенсированным рейтингом)")
                    st.write(f"• **{successfully_grouped_input_items} входных товаров** из {processed_count} товаров с связями обработано успешно")
                    
                    # Проверяем корректность: все входные товары с связями должны быть в группах
                    if processed_count != successfully_grouped_input_items:
                        st.warning(f"⚠️ Есть расхождение в подсчетах товаров! Ожидалось {processed_count}, обработано {successfully_grouped_input_items}")
                else:
                    st.info("ℹ️ Групп не создано")
            
            # Summary and recommendations
            st.markdown("---")
            st.markdown("### 💡 Рекомендации")
            
            if not saved_groups_df.empty:
                # Правильное вычисление success_rate: только входные товары пользователя
                input_wb_skus = set()
                if 'advanced_input_wb_skus' in st.session_state:
                    input_wb_skus = set(st.session_state['advanced_input_wb_skus'])
                
                if input_wb_skus:
                    # Считаем сколько входных товаров попало в группы
                    successfully_grouped_count = 0
                    for wb_sku in input_wb_skus:
                        if str(wb_sku) in saved_groups_df['wb_sku'].astype(str).values or \
                           str(wb_sku) in saved_groups_df['merge_on_card'].astype(str).values:
                            successfully_grouped_count += 1
                    
                    success_rate = (successfully_grouped_count / len(input_wb_skus)) * 100
                else:
                    # Fallback: используем старую логику если нет данных о входных товарах
                    success_rate = len(saved_groups_df['wb_sku'].unique()) / saved_wb_skus_count * 100
                
                if success_rate >= 80:
                    st.success(f"🎉 **Отличный результат!** {success_rate:.1f}% входных товаров успешно сгруппированы")
                elif success_rate >= 60:
                    st.info(f"👍 **Хороший результат!** {success_rate:.1f}% входных товаров успешно сгруппированы")
                else:
                    st.warning(f"⚠️ **Требует внимания:** Только {success_rate:.1f}% входных товаров успешно сгруппированы")
            
            if not saved_no_links_df.empty:
                st.info("💡 **Для товаров без связей:** Проверьте наличие общих штрихкодов с товарами Ozon")
            
            if not saved_low_rating_df.empty:
                st.info("💡 **Для товаров с низким рейтингом:** Рассмотрите снижение минимального рейтинга или поиск дополнительных аналогичных товаров")
            
            # Кнопка очистки результатов
            st.markdown("---")
            if st.button("🗑️ Очистить результаты", key="clear_advanced_results", help="Очистить результаты и начать новую обработку"):
                for key in ['advanced_results_ready', 'advanced_groups_df', 'advanced_no_links_df', 'advanced_low_rating_df', 'advanced_results_timestamp', 'advanced_wb_skus_count', 'advanced_min_group_rating', 'advanced_input_wb_skus']:
                    if key in st.session_state:
                        del st.session_state[key]
                st.rerun()
        
        else:
            st.info("👆 **Введите wb_sku выше для начала работы с расширенной группировкой**")

with tab4:
    st.header("✏️ Редактирование существующих групп")
    
    st.markdown("""
    ### 🎯 Назначение раздела
    Этот раздел позволяет работать с уже существующими группами товаров из таблицы `oz_category_products`.
    
    **Возможности:**
    - Просмотр статистики существующих групп
    - Поиск и фильтрация групп по различным критериям
    - Детальный анализ группы с поиском связей с товарами WB
    - Просмотр данных для редактирования групп
    """)
    
    # Import the helper functions
    from utils.existing_groups_helpers import (
        get_existing_groups_statistics,
        get_existing_groups_list,
        get_group_details_with_wb_connections,
        analyze_group_wb_connections,
        search_groups_by_criteria,
        update_oz_sku_from_oz_products,
        get_oz_sku_update_statistics,
        get_group_products_details
    )
    
    # --- Statistics Section ---
    st.markdown("---")
    st.subheader("📊 Статистика существующих групп")
    
    with st.spinner("Загрузка статистики..."):
        stats = get_existing_groups_statistics(conn)
    
    if stats:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📦 Всего товаров", stats.get('total_products', 0))
        
        with col2:
            st.metric("🔗 Всего групп", stats.get('total_groups', 0))
        
        with col3:
            st.metric("👥 Групп с 2+ товарами", stats.get('groups_with_2_plus', 0))
        
        with col4:
            avg_size = stats.get('avg_group_size', 0)
            st.metric("📏 Средний размер группы", f"{avg_size}" if avg_size > 0 else "N/A")
        
        # Additional metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("🏷️ Уникальных vendor кодов", stats.get('unique_vendor_codes', 0))
        
        with col2:
            st.metric("🆔 Товаров с OZ SKU", stats.get('products_with_oz_sku', 0))
        
        with col3:
            st.metric("📈 Макс. размер группы", stats.get('max_group_size', 0))
        
        with col4:
            st.metric("📉 Мин. размер группы", stats.get('min_group_size', 0))
        
        # Group size distribution
        if stats.get('groups_with_2_plus', 0) > 0:
            st.markdown("### 📊 Распределение групп по размеру")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                small = stats.get('small_groups', 0)
                st.metric("🔹 Малые группы (2-5)", small)
            
            with col2:
                medium = stats.get('medium_groups', 0)
                st.metric("🔷 Средние группы (6-15)", medium)
            
            with col3:
                large = stats.get('large_groups', 0)
                st.metric("🔶 Большие группы (16+)", large)
    else:
        st.info("📭 Данные о группах не найдены или таблица oz_category_products пуста")
    
    # --- OZ SKU Update Section ---
    st.markdown("---")
    st.subheader("🔧 Обновление поля oz_sku")
    
    st.markdown("""
    **Назначение**: Заполнение пустых значений `oz_sku` в таблице `oz_category_products` 
    на основе совпадений `oz_vendor_code` с таблицей `oz_products`.
    """)
    
    # Get update statistics
    with st.spinner("Получение статистики для обновления..."):
        update_stats = get_oz_sku_update_statistics(conn)
    
    if update_stats and 'error' not in update_stats:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📊 Всего записей", update_stats.get('total_records', 0))
        
        with col2:
            empty_count = update_stats.get('empty_oz_sku', 0)
            st.metric("❌ Пустых oz_sku", empty_count)
        
        with col3:
            filled_count = update_stats.get('filled_oz_sku', 0)
            st.metric("✅ Заполненных oz_sku", filled_count)
        
        with col4:
            can_update = update_stats.get('can_be_updated', 0)
            st.metric("🔄 Можно обновить", can_update)
        
        # Additional info
        col1, col2 = st.columns(2)
        
        with col1:
            unique_codes = update_stats.get('unique_vendor_codes', 0)
            st.metric("🏷️ Уникальных vendor кодов", unique_codes)
        
        with col2:
            matching_codes = update_stats.get('matching_vendor_codes_in_oz_products', 0)
            st.metric("🔗 Совпадений с oz_products", matching_codes)
        
        # Update button
        if can_update > 0:
            st.markdown("---")
            col1, col2 = st.columns([1, 2])
            
            with col1:
                if st.button("🚀 Обновить oz_sku", type="primary"):
                    with st.spinner("Обновление oz_sku... Пожалуйста, подождите..."):
                        update_result = update_oz_sku_from_oz_products(conn)
                    
                    if update_result.get('success'):
                        st.success(f"✅ {update_result['message']}")
                        
                        # Show detailed results
                        if update_result.get('updated_count', 0) > 0:
                            st.info(f"📊 **Детали обновления:**")
                            st.write(f"- Всего записей: {update_result.get('total_records', 0)}")
                            st.write(f"- Было пустых oz_sku: {update_result.get('empty_oz_sku', 0)}")
                            st.write(f"- Потенциальных обновлений: {update_result.get('potential_updates', 0)}")
                            st.write(f"- **Фактически обновлено: {update_result.get('updated_count', 0)}**")
                            
                            st.balloons()
                            
                            # Suggest refreshing the page
                            st.info("💡 **Рекомендация**: Обновите страницу для просмотра актуальной статистики")
                    else:
                        st.error(f"❌ {update_result.get('error', 'Неизвестная ошибка')}")
            
            with col2:
                st.warning(f"⚠️ **Внимание**: Будет обновлено {can_update} записей с пустым oz_sku")
                st.info("💡 **Принцип работы**: Поиск совпадений по oz_vendor_code между таблицами oz_category_products и oz_products")
        else:
            if empty_count == 0:
                st.success("✅ **Все записи уже имеют заполненное поле oz_sku**")
            else:
                st.info("ℹ️ **Нет совпадений с таблицей oz_products для обновления пустых oz_sku**")
    
    elif 'error' in update_stats:
        st.error(f"❌ {update_stats['error']}")
    
    # --- Search and Filter Section ---
    st.markdown("---")
    st.subheader("🔍 Поиск и фильтрация групп")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        search_text = st.text_input(
            "🔎 Поиск по тексту",
            placeholder="Введите текст для поиска...",
            help="Поиск в названиях товаров, vendor кодах и merge_on_card"
        )
    
    with col2:
        min_group_size = st.number_input(
            "📏 Мин. размер группы",
            min_value=1,
            max_value=100,
            value=2,
            help="Минимальное количество товаров в группе"
        )
    
    with col3:
        max_group_size = st.number_input(
            "📐 Макс. размер группы",
            min_value=1,
            max_value=1000,
            value=100,
            help="Максимальное количество товаров в группе"
        )
    
    # Search button
    if st.button("🔍 Найти группы", type="primary"):
        with st.spinner("Поиск групп..."):
            search_results = search_groups_by_criteria(
                conn, 
                search_text=search_text,
                min_group_size=min_group_size,
                max_group_size=max_group_size
            )
        
        if not search_results.empty:
            st.success(f"✅ Найдено {len(search_results)} групп")
            
            # Display results
            st.markdown("### 📋 Результаты поиска")
            
            # Configure columns for display
            column_config = {
                'merge_on_card': st.column_config.TextColumn('🔗 Группа (merge_on_card)', width="medium"),
                'group_size': st.column_config.NumberColumn('📊 Размер', width="small"),
                'unique_vendor_codes': st.column_config.NumberColumn('🏷️ Уник. кодов', width="small"),
                'vendor_codes_sample': st.column_config.TextColumn('📝 Примеры кодов', width="large"),
                'product_names_sample': st.column_config.TextColumn('🛍️ Примеры названий', width="large")
            }
            
            # Show results table (without selection)
            st.dataframe(
                search_results,
                use_container_width=True,
                hide_index=True,
                column_config=column_config
            )
            
            # Group selection via selectbox
            st.markdown("### 🎯 Выбор группы для анализа")
            
            # Prepare options for selectbox
            group_options = ["Выберите группу..."] + [
                f"{row['merge_on_card']} (размер: {row['group_size']})" 
                for _, row in search_results.iterrows()
            ]
            
            selected_option = st.selectbox(
                "Выберите группу для детального анализа:",
                options=group_options,
                key="group_selector_search"
            )
            
            if selected_option != "Выберите группу...":
                # Extract merge_on_card from selected option
                selected_merge_on_card = selected_option.split(" (размер:")[0]
                
                st.markdown("---")
                st.subheader(f"🔍 Детальный анализ группы: `{selected_merge_on_card}`")
                
                st.markdown("#### 🛍️ Все товары в группе")
                
                # Get group products
                with st.spinner("Загрузка товаров группы..."):
                    group_products = get_group_products_details(conn, selected_merge_on_card)
                
                if not group_products.empty:
                    # Show group summary with unique WB SKU information
                    col1, col2, col3, col4, col5 = st.columns(5)
                    
                    with col1:
                        st.metric("📦 Товаров в группе", len(group_products))
                    
                    with col2:
                        wb_connections = len(group_products[group_products['wb_sku'].notna()]) if 'wb_sku' in group_products.columns else 0
                        st.metric("🔗 Связей с WB", wb_connections)
                    
                    with col3:
                        unique_wb_skus = group_products['wb_sku'].dropna().nunique() if 'wb_sku' in group_products.columns else 0
                        st.metric("🆔 Уникальных WB SKU", unique_wb_skus)
                    
                    with col4:
                        avg_price = group_products['oz_actual_price'].mean() if 'oz_actual_price' in group_products.columns else 0
                        st.metric("💰 Средняя цена", f"{avg_price:.2f} ₽" if avg_price > 0 else "N/A")
                    
                    with col5:
                        total_stock = group_products['oz_fbo_stock'].sum() if 'oz_fbo_stock' in group_products.columns else 0
                        st.metric("📦 Общий остаток", int(total_stock) if total_stock > 0 else 0)
                        
                        # Configure columns for group products display
                        group_column_config = {
                            'merge_on_card': st.column_config.TextColumn('🔗 Группа', width="medium"),
                            'wb_sku': st.column_config.TextColumn('🛒 WB SKU', width="medium"),
                            'oz_vendor_code': st.column_config.TextColumn('🏷️ OZ Vendor Code', width="medium"),
                            'oz_sku': st.column_config.TextColumn('🆔 OZ SKU', width="medium"),
                            'oz_fbo_stock': st.column_config.NumberColumn('📦 Остаток', width="small"),
                            'product_name': st.column_config.TextColumn('🛍️ Название товара', width="large"),
                            'barcode': st.column_config.TextColumn('🔢 Штрихкод', width="medium"),
                            'oz_actual_price': st.column_config.NumberColumn('💰 Цена', format="%.2f ₽", width="small")
                        }
                        
                        # Display group products table
                        st.dataframe(
                            group_products,
                            use_container_width=True,
                            hide_index=True,
                            column_config=group_column_config
                        )
                        
                        # Используем компонент для экспорта товаров группы
                        render_export_controls(
                            group_products, 
                            f"group_products_{selected_merge_on_card}_top", 
                            "товары группы"
                        )
                    
                else:
                    st.warning("⚠️ Товары в группе не найдены")
            
            else:
                st.info("👆 **Выберите группу из таблицы выше для детального анализа**")
        
        else:
            st.warning("⚠️ Группы по указанным критериям не найдены")
    
    else:
        # Show default list without search
        if stats and stats.get('groups_with_2_plus', 0) > 0:
            st.markdown("### 📋 Топ-20 групп по размеру")
            
            with st.spinner("Загрузка списка групп..."):
                default_groups = get_existing_groups_list(conn, min_group_size=2)
            
            if not default_groups.empty:
                # Show only top 20
                top_groups = default_groups.head(20)
                
                column_config = {
                    'merge_on_card': st.column_config.TextColumn('🔗 Группа (merge_on_card)', width="medium"),
                    'group_size': st.column_config.NumberColumn('📊 Размер', width="small"),
                    'unique_vendor_codes': st.column_config.NumberColumn('🏷️ Уник. кодов', width="small"),
                    'vendor_codes_sample': st.column_config.TextColumn('📝 Примеры кодов', width="large"),
                    'product_names_sample': st.column_config.TextColumn('🛍️ Примеры названий', width="large")
                }
                
                # Show table (without selection)
                st.dataframe(
                    top_groups,
                    use_container_width=True,
                    hide_index=True,
                    column_config=column_config
                )
                
                # Group selection via selectbox
                st.markdown("### 🎯 Выбор группы для анализа")
                
                # Prepare options for selectbox
                top_group_options = ["Выберите группу..."] + [
                    f"{row['merge_on_card']} (размер: {row['group_size']})" 
                    for _, row in top_groups.iterrows()
                ]
                
                selected_top_option = st.selectbox(
                    "Выберите группу для детального анализа:",
                    options=top_group_options,
                    key="group_selector_top"
                )
                
                if selected_top_option != "Выберите группу...":
                    # Extract merge_on_card from selected option
                    selected_merge_on_card = selected_top_option.split(" (размер:")[0]
                    
                    st.markdown("---")
                    st.subheader(f"🔍 Детальный анализ группы: `{selected_merge_on_card}`")
                    
                    st.markdown("#### 🛍️ Все товары в группе")
                    
                    # Get group products
                    with st.spinner("Загрузка товаров группы..."):
                        group_products = get_group_products_details(conn, selected_merge_on_card)
                    
                    if not group_products.empty:
                        # Show group summary with unique WB SKU information
                        col1, col2, col3, col4, col5 = st.columns(5)
                        
                        with col1:
                            st.metric("📦 Товаров в группе", len(group_products))
                        
                        with col2:
                            wb_connections = len(group_products[group_products['wb_sku'].notna()]) if 'wb_sku' in group_products.columns else 0
                            st.metric("🔗 Связей с WB", wb_connections)
                        
                        with col3:
                            unique_wb_skus = group_products['wb_sku'].dropna().nunique() if 'wb_sku' in group_products.columns else 0
                            st.metric("🆔 Уникальных WB SKU", unique_wb_skus)
                        
                        with col4:
                            avg_price = group_products['oz_actual_price'].mean() if 'oz_actual_price' in group_products.columns else 0
                            st.metric("💰 Средняя цена", f"{avg_price:.2f} ₽" if avg_price > 0 else "N/A")
                        
                        with col5:
                            total_stock = group_products['oz_fbo_stock'].sum() if 'oz_fbo_stock' in group_products.columns else 0
                            st.metric("📦 Общий остаток", int(total_stock) if total_stock > 0 else 0)
                            
                            # Configure columns for group products display
                            group_column_config = {
                                'merge_on_card': st.column_config.TextColumn('🔗 Группа', width="medium"),
                                'wb_sku': st.column_config.TextColumn('🛒 WB SKU', width="medium"),
                                'oz_vendor_code': st.column_config.TextColumn('🏷️ OZ Vendor Code', width="medium"),
                                'oz_sku': st.column_config.TextColumn('🆔 OZ SKU', width="medium"),
                                'oz_fbo_stock': st.column_config.NumberColumn('📦 Остаток', width="small"),
                                'product_name': st.column_config.TextColumn('🛍️ Название товара', width="large"),
                                'barcode': st.column_config.TextColumn('🔢 Штрихкод', width="medium"),
                                'oz_actual_price': st.column_config.NumberColumn('💰 Цена', format="%.2f ₽", width="small")
                            }
                            
                            # Display group products table
                            st.dataframe(
                                group_products,
                                use_container_width=True,
                                hide_index=True,
                                column_config=group_column_config
                            )
                            
                            # Используем компонент для экспорта товаров группы
                            render_export_controls(
                                group_products, 
                                f"group_products_{selected_merge_on_card}_top", 
                                "товары группы"
                            )
                        
                    else:
                        st.warning("⚠️ Товары в группе не найдены")
                    
                    st.info("💡 **Для поиска конкретных групп используйте фильтры выше**")

# --- Current Data Statistics ---
if conn:
    st.markdown("---")
    
    try:
        # Check if table exists and get statistics
        rating_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT oz_sku) as unique_skus,
                AVG(rating) as avg_rating,
                MIN(rating) as min_rating,
                MAX(rating) as max_rating,
                SUM(rev_number) as total_reviews
            FROM oz_card_rating
        """).fetchone()
        
        if rating_stats and rating_stats[0] > 0:
            # Подготавливаем данные для компонента статистики
            stats = {
                'total_records': rating_stats[0],
                'unique_skus': rating_stats[1],
                'avg_rating': rating_stats[2],
                'total_reviews': rating_stats[5] if rating_stats[5] else 0
            }
            
            # Используем компонент для отображения статистики
            render_statistics_metrics(stats, "📈 Статистика текущих данных")
            
            # Rating distribution
            with st.expander("📊 Распределение рейтингов", expanded=False):
                rating_dist = conn.execute("""
                    SELECT rating, COUNT(*) as count
                    FROM oz_card_rating
                    WHERE rating IS NOT NULL
                    GROUP BY rating
                    ORDER BY rating
                """).fetchdf()
                
                if not rating_dist.empty:
                    st.bar_chart(rating_dist.set_index('rating'))
                else:
                    st.info("Нет данных для отображения распределения")
        else:
            st.info("📭 Данные рейтингов еще не загружены. Используйте вкладку 'Загрузка рейтингов Ozon' для импорта данных.")
            
    except Exception as e:
        if "no such table" in str(e).lower():
            st.info("📭 Таблица рейтингов еще не создана. Данные будут доступны после первого импорта.")
        else:
            st.warning(f"⚠️ Не удалось получить статистику: {str(e)}")

# Close connection
if conn:
    conn.close() 