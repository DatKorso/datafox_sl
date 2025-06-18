"""
Streamlit page for Cards Matcher - товарные карточки.

Эта страница предоставляет функционал для:
- Загрузки рейтингов товаров Ozon
- Анализа и группировки товаров по общим параметрам
- Рекомендаций по объединению/разделению товарных карточек
"""
import streamlit as st
import pandas as pd
import os
from utils.db_connection import connect_db
from utils.config_utils import get_report_path, set_report_path, get_data_filter
from utils.db_crud import import_data_from_dataframe
from utils.data_cleaner import apply_data_cleaning
from utils.db_schema import get_table_schema_definition

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
tab1, tab2 = st.tabs(["📊 Загрузка рейтингов Ozon", "🔗 Группировка товаров (в разработке)"])

with tab1:
    st.header("📊 Загрузка рейтингов товаров Ozon")
    
    st.markdown("""
    ### Требования к файлу
    - Формат: `.xlsx`
    - Первая строка: заголовки колонок
    - Вторая строка и далее: данные
    
    ### Маппинг колонок
    | Колонка в файле | Поле в БД | Описание |
    |---|---|---|
    | `RezonitemID` | `oz_sku` | SKU товара на Ozon |
    | `Артикул` | `oz_vendor_code` | Артикул поставщика |
    | `Рейтинг (1)` | `rating` | Рейтинг товара (1-5) |
    | `Кол-во отзывов` | `rev_number` | Количество отзывов |
    """)
    
    # --- Brand Filter Information ---
    st.markdown("---")
    st.subheader("🎯 Настройки фильтрации брендов")
    
    brands_filter = get_data_filter("oz_category_products_brands")
    
    if brands_filter and brands_filter.strip():
        allowed_brands = [brand.strip() for brand in brands_filter.split(";") if brand.strip()]
        if allowed_brands:
            st.success(f"✅ **Фильтр брендов активен**: Будут загружены только товары следующих брендов:")
            st.info("📋 **Разрешенные бренды**: " + ", ".join(allowed_brands))
            
            with st.expander("💡 Как изменить фильтр брендов"):
                st.markdown("""
                1. Перейдите в раздел **Settings** (страница настроек)
                2. Найдите секцию **Data Filters**
                3. Измените поле **Ozon Category Products - Brands Filter**
                4. Сохраните настройки
                
                **Формат**: `Бренд1;Бренд2;Бренд3` (разделяйте точкой с запятой)
                """)
        else:
            st.info("🔍 **Фильтр брендов не настроен** - будут загружены рейтинги всех товаров")
    else:
        st.info("🔍 **Фильтр брендов не настроен** - будут загружены рейтинги всех товаров")
        
        with st.expander("⚙️ Как настроить фильтр брендов"):
            st.markdown("""
            Для фильтрации данных по конкретным брендам:
            
            1. Перейдите в раздел **Settings** (страница настроек)
            2. Найдите секцию **Data Filters**
            3. В поле **Ozon Category Products - Brands Filter** укажите нужные бренды
            4. Сохраните настройки
            
            **Пример**: `Shuzzi;Nike;Adidas` - загрузятся только товары этих брендов
            """)
    
    # --- File Selection ---
    st.subheader("📁 Выбор файла с рейтингами")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Get default path from settings
        default_path = get_report_path("oz_card_rating_xlsx")
        
        # File selection method
        file_source = st.radio(
            "Как выбрать файл:",
            ["📂 Использовать путь из настроек", "⬆️ Загрузить файл"],
            index=0 if default_path else 1,
            key="rating_file_source"
        )
    
    with col2:
        st.info("💡 **Совет**: Настройте путь к файлу в разделе Settings для быстрого доступа")
    
    selected_file_path = None
    
    if file_source == "📂 Использовать путь из настроек":
        if default_path:
            if os.path.exists(default_path):
                selected_file_path = default_path
                st.success(f"✅ Файл найден: `{os.path.basename(default_path)}`")
                st.caption(f"Полный путь: {default_path}")
            else:
                st.error(f"❌ Файл не найден по пути: {default_path}")
                st.info("Проверьте путь в настройках или выберите другой способ загрузки файла.")
        else:
            st.warning("⚠️ Путь к файлу не настроен.")
            
            # Allow setting path directly
            st.subheader("⚙️ Настройка пути к файлу")
            new_path = st.text_input(
                "Введите путь к файлу с рейтингами Ozon:",
                placeholder="/path/to/ozon_ratings.xlsx",
                help="Укажите полный путь к файлу .xlsx с рейтингами товаров"
            )
            
            if st.button("💾 Сохранить путь", key="save_rating_path"):
                if new_path and new_path.strip():
                    try:
                        set_report_path("oz_card_rating_xlsx", new_path.strip())
                        st.success("✅ Путь сохранен в настройках!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Ошибка сохранения пути: {e}")
                else:
                    st.warning("⚠️ Введите корректный путь к файлу")
    
    else:  # Upload file
        uploaded_file = st.file_uploader(
            "Выберите Excel файл с рейтингами Ozon:",
            type=['xlsx'],
            help="Файл должен содержать колонки RezonitemID, Артикул, Рейтинг (1), Кол-во отзывов",
            key="rating_file_uploader"
        )
        
        if uploaded_file is not None:
            # Save uploaded file temporarily
            temp_path = f"temp_oz_rating_{uploaded_file.name}"
            with open(temp_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            selected_file_path = temp_path
            st.success(f"✅ Файл загружен: `{uploaded_file.name}`")
    
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
                                # Display quick stats
                                col1, col2, col3 = st.columns(3)
                                with col1:
                                    st.metric("📊 Строк с данными", len(import_df))
                                with col2:
                                    unique_skus = len(import_df.get('RezonitemID', pd.Series()).dropna().unique())
                                    st.metric("🏷️ Уникальных SKU", unique_skus)
                                with col3:
                                    avg_rating = import_df.get('Рейтинг (1)', pd.Series()).mean()
                                    if pd.notna(avg_rating):
                                        st.metric("⭐ Средний рейтинг", f"{avg_rating:.2f}")
                                    else:
                                        st.metric("⭐ Средний рейтинг", "N/A")
                                
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
                                        st.success("✅ Рейтинги успешно импортированы!")
                                        st.info(f"📊 Импортировано {count} записей в таблицу oz_card_rating")
                                        
                                        # Show preview of imported data
                                        with st.expander("👀 Предпросмотр импортированных данных (первые 10 строк)"):
                                            st.dataframe(import_df.head(10), use_container_width=True, hide_index=True)
                                        
                                        # Cleanup temp file if it was uploaded
                                        if selected_file_path.startswith("temp_"):
                                            try:
                                                os.remove(selected_file_path)
                                            except:
                                                pass
                                    else:
                                        st.error(f"❌ Ошибка при импорте данных: {error_message}")
                        else:
                            st.error("❌ Файл не содержит данных для импорта")
                            
                    except Exception as e:
                        st.error(f"❌ Ошибка импорта: {str(e)}")
                        st.info("Проверьте формат файла и структуру данных")
        
        # Add info section about what happens during import
        with st.expander("ℹ️ Что происходит при импорте"):
            st.markdown("""
            **Этапы импорта:**
            1. 📖 **Чтение файла Excel** с поддержкой десятичных рейтингов (запятая как разделитель)
            2. ✅ **Валидация данных** - проверка наличия всех обязательных колонок
            3. 🎯 **Применение фильтра брендов** (если настроен в Settings)
            4. 🗑️ **Очистка таблицы** - удаление предыдущих данных рейтингов
            5. 💾 **Импорт новых данных** в таблицу oz_card_rating
            6. ✅ **Подтверждение успеха** с показом статистики
            
            **Обрабатываемые типы данных:**
            - `RezonitemID` → BIGINT (SKU товара)
            - `Артикул` → VARCHAR (артикул поставщика)  
            - `Рейтинг (1)` → DECIMAL(3,2) (рейтинг 0.00-5.00)
            - `Кол-во отзывов` → INTEGER (количество отзывов)
            """)
    else:
        st.info("👆 **Выберите файл с рейтингами выше для начала импорта**")

with tab2:
    st.header("🔗 Группировка товаров")
    
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
            get_wb_sku_ratings_with_oz_data, get_punta_table_data_for_wb_skus,
            create_product_groups, get_available_grouping_columns, analyze_group_quality,
            test_wb_sku_connections
        )
        
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
        
        col1, col2 = st.columns(2)
        
        with col1:
            min_group_rating = st.slider(
                "Минимальный рейтинг группы", 
                0.0, 5.0, 3.0, 0.1,
                help="Из групп будут исключены wb_sku с низким рейтингом, чтобы минимальный рейтинг группы соответствовал этому значению"
            )
        
        with col2:
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
                    
                    # Step 1: Get WB SKU ratings
                    wb_ratings_df = get_wb_sku_ratings_with_oz_data(conn, wb_skus)
                    
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
                            min_group_rating=min_group_rating
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
                                
                                # Show detailed results table
                                st.markdown("### 📋 Детальная информация по группам")
                                
                                # Prepare display columns
                                display_columns = [
                                    'group_id', 'wb_sku', 'avg_rating', 'oz_sku_count', 
                                    'oz_skus_list', 'total_reviews', 'wb_count', 
                                    'group_avg_rating', 'total_oz_sku_count', 'group_total_reviews',
                                    'group_recommendation'
                                ]
                                
                                # Add grouping columns if they exist
                                if grouping_columns:
                                    for col in grouping_columns:
                                        if col in groups_df.columns:
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
                                
                                # Export functionality
                                st.markdown("---")
                                col1, col2 = st.columns(2)
                                
                                with col1:
                                    if st.button("📥 Экспортировать результаты в Excel"):
                                        try:
                                            from datetime import datetime
                                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                            filename = f"product_groups_{timestamp}.xlsx"
                                            
                                            groups_df.to_excel(filename, index=False)
                                            
                                            with open(filename, "rb") as file:
                                                st.download_button(
                                                    label="Скачать файл Excel",
                                                    data=file.read(),
                                                    file_name=filename,
                                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                                )
                                        except Exception as e:
                                            st.error(f"Ошибка при экспорте: {e}")
                                
                                with col2:
                                    st.info("💡 **Совет**: Используйте рекомендации для оптимизации групп товаров на маркетплейсе")
        
        else:
            st.info("👆 **Введите wb_sku выше для начала работы с группировкой товаров**")

# --- Current Data Statistics ---
if conn:
    st.markdown("---")
    st.subheader("📈 Статистика текущих данных")
    
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
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("📊 Записей рейтингов", rating_stats[0])
            with col2:
                st.metric("🏷️ Уникальных SKU", rating_stats[1])
            with col3:
                avg_rating = rating_stats[2]
                st.metric("⭐ Средний рейтинг", f"{avg_rating:.2f}" if avg_rating else "N/A")
            with col4:
                st.metric("💬 Всего отзывов", rating_stats[5] if rating_stats[5] else 0)
            
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