"""
Страница для сбора wb_sku по текущему ассортименту товаров Озон.

Функциональность:
- Сбор wb_sku для всего ассортимента Озон или выбранных oz_sku
- Использование алгоритма поиска актуальных штрихкодов
- Анализ дубликатов и товаров без связей
- Экспорт результатов в Excel

Автор: DataFox SL Project
Версия: 1.0.0
"""

import streamlit as st
import pandas as pd
import duckdb
from datetime import datetime
import io
import time
from utils.db_connection import get_connection_and_ensure_schema
from utils.oz_to_wb_collector import (
    OzToWbCollector, 
    WbSkuCollectionResult,
    collect_wb_skus_for_all_oz_products,
    collect_wb_skus_for_oz_list
)
from utils import config_utils

# Настройка страницы
st.set_page_config(
    page_title="Сбор WB SKU по Озон",
    page_icon="🔗",
    layout="wide"
)

st.title("🔗 Сбор WB SKU по Ассортименту Озон")
st.markdown("""
Этот инструмент собирает список wb_sku для вашего текущего ассортимента товаров Озон, 
используя **алгоритм поиска актуальных штрихкодов** для точного связывания карточек.
""")

# Отображение информации о фильтрации по брендам
brands_filter = config_utils.get_data_filter("oz_category_products_brands")
if brands_filter and brands_filter.strip():
    allowed_brands = [brand.strip() for brand in brands_filter.split(";") if brand.strip()]
    if allowed_brands:
        st.info(f"🏷️ **Активная фильтрация по брендам**: {', '.join(allowed_brands)}")
        st.caption("Сбор будет выполнен только для товаров указанных брендов. Настройки можно изменить в разделе ⚙️ Настройки.")
else:
    st.warning("🔍 **Фильтр брендов не настроен** - будет обработан весь ассортимент")
    with st.expander("💡 Как настроить фильтр брендов"):
        st.markdown("""
        Для обработки только товаров определенных брендов:
        
        1. Перейдите в раздел **⚙️ Настройки**
        2. Найдите секцию **Data Filters**  
        3. В поле **Ozon Category Products - Brands Filter** укажите нужные бренды
        4. Сохраните настройки
        
        **Пример**: `Shuzzi;Nike;Adidas` - будут обработаны только товары этих брендов
        """)

# Инициализация базы данных
@st.cache_resource
def get_database_connection():
    """Инициализация подключения к базе данных"""
    return get_connection_and_ensure_schema()

db_conn = get_database_connection()

if not db_conn:
    st.error("❌ Не удалось подключиться к базе данных")
    st.stop()

# Боковая панель с настройками
st.sidebar.header("⚙️ Настройки сбора")

# Выбор режима работы
collection_mode = st.sidebar.radio(
    "Режим сбора:",
    ["🔄 Весь ассортимент Озон", "📝 Конкретные oz_sku"],
    help="Выберите обрабатывать ли все товары из oz_products или только указанные"
)

# Настройки для конкретных oz_sku
oz_skus_input = []
if collection_mode == "📝 Конкретные oz_sku":
    st.sidebar.subheader("📝 Список OZ SKU")
    
    # Способ ввода
    input_method = st.sidebar.radio(
        "Способ ввода oz_sku:",
        ["✍️ Ручной ввод", "📁 Загрузка файла"],
        help="Выберите как указать список oz_sku для обработки"
    )
    
    if input_method == "✍️ Ручной ввод":
        oz_skus_text = st.sidebar.text_area(
            "Введите oz_sku (по одному на строку):",
            height=200,
            help="Вставьте список oz_sku, каждый на новой строке"
        )
        
        if oz_skus_text.strip():
            oz_skus_input = [
                line.strip() for line in oz_skus_text.split('\n') 
                if line.strip() and line.strip().isdigit()
            ]
            st.sidebar.success(f"✅ Введено {len(oz_skus_input)} oz_sku")
    
    elif input_method == "📁 Загрузка файла":
        uploaded_file = st.sidebar.file_uploader(
            "Загрузите файл с oz_sku:",
            type=['txt', 'csv', 'xlsx'],
            help="Файл должен содержать oz_sku в первой колонке или по строчно"
        )
        
        if uploaded_file:
            try:
                if uploaded_file.name.endswith('.txt'):
                    content = uploaded_file.read().decode('utf-8')
                    oz_skus_input = [
                        line.strip() for line in content.split('\n') 
                        if line.strip() and line.strip().isdigit()
                    ]
                elif uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                    oz_skus_input = df.iloc[:, 0].astype(str).tolist()
                elif uploaded_file.name.endswith('.xlsx'):
                    df = pd.read_excel(uploaded_file)
                    oz_skus_input = df.iloc[:, 0].astype(str).tolist()
                
                oz_skus_input = [sku for sku in oz_skus_input if str(sku).strip().isdigit()]
                st.sidebar.success(f"✅ Загружено {len(oz_skus_input)} oz_sku")
                
            except Exception as e:
                st.sidebar.error(f"❌ Ошибка при загрузке файла: {e}")

# Дополнительные настройки
st.sidebar.subheader("🔧 Дополнительные настройки")

include_details = st.sidebar.checkbox(
    "Включить детальную информацию по WB SKU",
    value=True,
    help="Включает цены, остатки, бренды и категории для найденных wb_sku"
)

auto_export = st.sidebar.checkbox(
    "Автоматический экспорт в Excel",
    value=False,
    help="Автоматически предложить скачать результаты в Excel формате"
)

# Основная область
col1, col2 = st.columns([2, 1])

with col1:
    st.header("🚀 Запуск сбора")
    
    # Информация о настройках
    if collection_mode == "🔄 Весь ассортимент Озон":
        # Показываем количество товаров с учетом фильтрации
        if brands_filter and brands_filter.strip():
            allowed_brands = [brand.strip() for brand in brands_filter.split(";") if brand.strip()]
            if allowed_brands:
                brands_condition = ", ".join([f"'{brand}'" for brand in allowed_brands])
                count_query = f"SELECT COUNT(DISTINCT oz_sku) FROM oz_products WHERE oz_sku IS NOT NULL AND oz_brand IN ({brands_condition})"
            else:
                count_query = "SELECT COUNT(DISTINCT oz_sku) FROM oz_products WHERE oz_sku IS NOT NULL"
        else:
            count_query = "SELECT COUNT(DISTINCT oz_sku) FROM oz_products WHERE oz_sku IS NOT NULL"
        
        try:
            total_count = db_conn.execute(count_query).fetchone()[0]
            if brands_filter and brands_filter.strip():
                allowed_brands = [brand.strip() for brand in brands_filter.split(";") if brand.strip()]
                if allowed_brands:
                    st.info(f"📊 Будет обработано {total_count:,} oz_sku из отфильтрованного ассортимента брендов: {', '.join(allowed_brands[:3])}{'...' if len(allowed_brands) > 3 else ''}")
                else:
                    st.info(f"📊 Будет обработано {total_count:,} oz_sku из всего ассортимента")
            else:
                st.info(f"📊 Будет обработано {total_count:,} oz_sku из всего ассортимента")
        except Exception as e:
            st.info("📊 Будет обработан ассортимент из таблицы oz_products")
    else:
        if oz_skus_input:
            st.success(f"📝 Готово к обработке {len(oz_skus_input)} oz_sku")
        else:
            st.warning("⚠️ Укажите oz_sku для обработки")

with col2:
    st.header("ℹ️ Алгоритм")
    st.markdown("""
    **Актуальные штрихкоды:**
    - **WB**: первый в списке через `;`
    - **OZ**: последний по порядку добавления в базу
    
    **Результат:**
    - Уникальные wb_sku
    - OZ без связей с WB  
    - Дубликаты связей
    """)

# Кнопка запуска
if st.button("🔗 Запустить сбор WB SKU", type="primary", use_container_width=True):
    
    # Валидация входных данных
    if collection_mode == "📝 Конкретные oz_sku" and not oz_skus_input:
        st.error("❌ Не указаны oz_sku для обработки")
        st.stop()
    
    # Запуск сбора с прогрессом
    progress_bar = st.progress(0)
    progress_text = st.empty()
    
    def update_progress(progress: float, message: str):
        """Обновляет прогресс-бар и текст"""
        progress_bar.progress(progress)
        progress_text.text(message)
    
    try:
        if collection_mode == "🔄 Весь ассортимент Озон":
            result = collect_wb_skus_for_all_oz_products(db_conn, update_progress)
        else:
            result = collect_wb_skus_for_oz_list(db_conn, oz_skus_input, update_progress)
        
        # Завершаем прогресс
        progress_bar.progress(1.0)
        progress_text.text("✅ Сбор завершен!")
        
        # Сохраняем результат в сессии
        st.session_state.collection_result = result
        st.session_state.collection_timestamp = datetime.now()
        
        # Убираем прогресс-бар после завершения
        time.sleep(1)
        progress_bar.empty()
        progress_text.empty()
        
    except Exception as e:
        progress_bar.empty()
        progress_text.empty()
        st.error(f"❌ Ошибка при сборе wb_sku: {e}")
        st.stop()

# Отображение результатов
if 'collection_result' in st.session_state:
    result: WbSkuCollectionResult = st.session_state.collection_result
    timestamp = st.session_state.collection_timestamp
    
    st.success("✅ Сбор wb_sku завершен успешно!")
    st.caption(f"🕒 Время выполнения: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Статистика
    st.header("📊 Статистика сбора")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "🎯 Уникальных WB SKU",
            result.stats['unique_wb_skus_found'],
            help="Количество уникальных wb_sku найденных для ассортимента"
        )
    
    with col2:
        oz_with_wb = result.stats['total_oz_skus_processed'] - len(result.no_links_oz_skus)
        st.metric(
            "✅ OZ SKU со связями",
            oz_with_wb,
            help="Количество oz_sku для которых найдены соответствующие wb_sku"
        )
    
    with col3:
        st.metric(
            "❌ OZ SKU без связей",
            len(result.no_links_oz_skus),
            help="Количество oz_sku для которых НЕ найдены wb_sku"
        )
    
    with col4:
        st.metric(
            "🔀 Дубликаты связей",
            len(result.duplicate_mappings),
            help="Количество oz_sku связанных с несколькими wb_sku"
        )
    
    # Детальные результаты
    st.header("📋 Детальные результаты")
    
    # Вкладки для разных типов результатов
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🎯 Найденные WB SKU", 
        "❌ OZ SKU без связей", 
        "🔀 Дубликаты связей",
        "📊 Полная статистика",
        "🔧 Отладочная информация"
    ])
    
    with tab1:
        st.subheader("🎯 Найденные WB SKU")
        
        if result.wb_skus:
            # Создаем DataFrame для отображения
            wb_skus_df = pd.DataFrame({
                'wb_sku': [str(sku) for sku in result.wb_skus]
            })
            
            st.dataframe(
                wb_skus_df,
                use_container_width=True,
                height=400
            )
            
            st.caption(f"Показано {len(result.wb_skus)} wb_sku")
        else:
            st.info("📭 WB SKU не найдены")
    
    with tab2:
        st.subheader("❌ OZ SKU без связей с WB")
        
        if result.no_links_oz_skus:
            oz_without_wb_df = pd.DataFrame({
                'oz_sku': [str(sku) for sku in result.no_links_oz_skus]
            })
            
            st.dataframe(
                oz_without_wb_df,
                use_container_width=True,
                height=400
            )
            
            st.caption(f"Показано {len(result.no_links_oz_skus)} oz_sku без связей")
        else:
            st.success("🎉 Все oz_sku имеют связи с wb_sku!")
    
    with tab3:
        st.subheader("🔀 Дубликаты связей")
        
        if result.duplicate_mappings:
            st.warning("⚠️ Обнаружены oz_sku связанные с несколькими wb_sku")
            
            # Преобразуем список словарей в DataFrame
            duplicates_df = pd.DataFrame(result.duplicate_mappings)
            
            st.dataframe(
                duplicates_df,
                use_container_width=True,
                height=400
            )
            
            st.caption(f"Показано {len(result.duplicate_mappings)} дублирующихся связей")
        else:
            st.success("✅ Дубликаты связей не обнаружены")
    
    with tab4:
        st.subheader("📊 Полная статистика")
        
        stats_df = pd.DataFrame([
            {'Метрика': 'Всего обработано OZ SKU', 'Значение': result.stats['total_oz_skus_processed']},
            {'Метрика': 'OZ SKU с актуальными штрихкодами', 'Значение': result.stats['oz_skus_with_barcodes']},
            {'Метрика': 'OZ SKU без связей WB', 'Значение': len(result.no_links_oz_skus)},
            {'Метрика': 'Уникальных WB SKU найдено', 'Значение': result.stats['unique_wb_skus_found']},
            {'Метрика': 'Дубликатов связей', 'Значение': len(result.duplicate_mappings)},
            {'Метрика': 'Всего совпадений штрихкодов', 'Значение': result.stats['total_barcode_matches']},
            {'Метрика': 'Время обработки (сек)', 'Значение': round(result.stats['processing_time_seconds'], 3)},
        ])
        
        st.dataframe(stats_df, use_container_width=True)
        
        # Процентные соотношения
        if result.stats['total_oz_skus_processed'] > 0:
            oz_with_wb = result.stats['total_oz_skus_processed'] - len(result.no_links_oz_skus)
            coverage_percent = (oz_with_wb / result.stats['total_oz_skus_processed']) * 100
            st.metric(
                "📈 Покрытие связями",
                f"{coverage_percent:.1f}%",
                help="Процент oz_sku для которых найдены wb_sku"
            )
    
    with tab5:
        st.subheader("🔧 Отладочная информация")
        
        if 'debug_info' in result.stats:
            debug_info = result.stats['debug_info']
            
            # Таймиги по этапам
            st.markdown("### ⏱️ Время выполнения этапов")
            
            timing_data = []
            total_time = debug_info.get('total_processing_time', 0)
            
            for step in range(1, 9):
                step_key = f'step{step}_time'
                if step_key in debug_info:
                    step_time = debug_info[step_key]
                    percentage = (step_time / total_time * 100) if total_time > 0 else 0
                    
                    step_names = {
                        1: "Получение OZ штрихкодов",
                        2: "Подготовка уникальных штрихкодов", 
                        3: "Подсчет записей WB",
                        4: "Поиск совпадений в WB (основной)",
                        5: "Обработка результатов",
                        6: "Создание маппинга",
                        7: "Поиск oz_sku без связей",
                        8: "Финализация"
                    }
                    
                    timing_data.append({
                        'Этап': f"{step}. {step_names.get(step, f'Этап {step}')}",
                        'Время (сек)': round(step_time, 3),
                        'Процент от общего времени': f"{percentage:.1f}%"
                    })
            
            timing_df = pd.DataFrame(timing_data)
            st.dataframe(timing_df, use_container_width=True)
            
            # Выделяем самый медленный этап
            if timing_data:
                slowest_step = max(timing_data, key=lambda x: x['Время (сек)'])
                st.warning(f"🐌 **Самый медленный этап:** {slowest_step['Этап']} ({slowest_step['Время (сек)']} сек, {slowest_step['Процент от общего времени']})")
            
            # Информация о данных
            st.markdown("### 📊 Информация о данных")
            
            data_info = []
            data_metrics = {
                'oz_skus_input': 'OZ SKU на входе',
                'oz_skus_with_barcodes': 'OZ SKU с штрихкодами',
                'unique_barcodes_count': 'Уникальных штрихкодов OZ',
                'wb_products_count': 'Записей в WB Products',
                'wb_individual_barcodes_count': 'Индивидуальных WB штрихкодов',
                'raw_matches_found': 'Сырых совпадений найдено',
                'unique_wb_skus_found': 'Уникальных WB SKU',
                'duplicate_mappings_count': 'Дубликатов связей',
                'oz_skus_without_links': 'OZ SKU без связей'
            }
            
            for key, label in data_metrics.items():
                if key in debug_info:
                    value = debug_info[key]
                    if isinstance(value, int) and value > 1000:
                        formatted_value = f"{value:,}"
                    else:
                        formatted_value = str(value)
                    data_info.append({
                        'Метрика': label,
                        'Значение': formatted_value
                    })
            
            data_df = pd.DataFrame(data_info)
            st.dataframe(data_df, use_container_width=True)
            
            # Анализ производительности
            st.markdown("### 🚀 Анализ производительности")
            
            if 'wb_products_count' in debug_info and 'unique_barcodes_count' in debug_info:
                wb_count = debug_info['wb_products_count']
                barcodes_count = debug_info['unique_barcodes_count']
                wb_individual_count = debug_info.get('wb_individual_barcodes_count', wb_count)
                sql_time = debug_info.get('sql_execution_time', 0)
                
                if sql_time > 0:
                    # Скорость обработки
                    wb_individual_per_sec = wb_individual_count / sql_time if sql_time > 0 else 0
                    barcodes_per_sec = barcodes_count / sql_time if sql_time > 0 else 0
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric(
                            "🔍 WB штрихкодов/сек", 
                            f"{wb_individual_per_sec:,.0f}",
                            help="Скорость обработки индивидуальных WB штрихкодов"
                        )
                    
                    with col2:
                        st.metric(
                            "📊 OZ штрихкодов/сек",
                            f"{barcodes_per_sec:.1f}",
                            help="Скорость обработки уникальных штрихкодов OZ"
                        )
                    
                    with col3:
                        # Новая сложность: JOIN операции вместо LIKE
                        complexity = wb_individual_count + barcodes_count
                        st.metric(
                            "🧮 JOIN операций",
                            f"{complexity:,}",
                            help="Количество записей для JOIN (WB штрихкоды + OZ штрихкоды)"
                        )
                    
                    # Рекомендации по оптимизации
                    old_complexity = wb_count * barcodes_count  # Старый алгоритм
                    new_complexity = complexity  # Новый алгоритм
                    optimization_ratio = old_complexity / new_complexity if new_complexity > 0 else 1
                    
                    st.info(f"🚀 **Оптимизация алгоритма:** Сложность снижена в {optimization_ratio:,.0f}x раз!")
                    st.markdown(f"""
                    - **Старый алгоритм:** {old_complexity:,} LIKE операций
                    - **Новый алгоритм:** {new_complexity:,} JOIN операций
                    """)
                    
                    if sql_time > 10:
                        st.error("🐌 **Все еще долгое выполнение!** Дополнительная оптимизация:")
                        st.markdown("""
                        - 📦 Батчинг OZ штрихкодов (обработка порциями)
                        - 🗃️ Создание временных индексов
                        - 💾 Кэширование промежуточных результатов
                        """)
                    elif sql_time > 5:
                        st.warning("⚠️ **Умеренно медленное выполнение.** Возможны дополнительные улучшения.")
                    else:
                        st.success("✅ **Отличная скорость выполнения после оптимизации!**")
        else:
            st.info("ℹ️ Отладочная информация недоступна для этого результата")
    
    # Экспорт результатов
    st.header("💾 Экспорт результатов")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📁 Скачать результаты в Excel", type="secondary"):
            try:
                # Создаем Excel файл в памяти
                output = io.BytesIO()
                
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    # Лист 1: Найденные wb_sku
                    if result.wb_skus:
                        wb_skus_df = pd.DataFrame({
                            'wb_sku': [str(sku) for sku in result.wb_skus]
                        })
                        wb_skus_df.to_excel(
                            writer, 
                            sheet_name='Found_WB_SKUs', 
                            index=False
                        )
                    
                    # Лист 2: OZ SKU без связей с WB
                    if result.no_links_oz_skus:
                        pd.DataFrame({
                            'oz_sku_without_wb_links': [str(sku) for sku in result.no_links_oz_skus]
                        }).to_excel(
                            writer, 
                            sheet_name='OZ_No_Links', 
                            index=False
                        )
                    
                    # Лист 3: Дубликаты связей
                    if result.duplicate_mappings:
                        duplicates_df = pd.DataFrame(result.duplicate_mappings)
                        duplicates_df.to_excel(
                            writer, 
                            sheet_name='Duplicate_Mappings', 
                            index=False
                        )
                    
                    # Лист 4: Статистика
                    stats_df.to_excel(writer, sheet_name='Statistics', index=False)
                
                output.seek(0)
                
                # Предлагаем скачать
                filename = f"wb_sku_collection_{timestamp.strftime('%Y%m%d_%H%M%S')}.xlsx"
                
                st.download_button(
                    label="⬇️ Скачать Excel файл",
                    data=output.getvalue(),
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                
                st.success("✅ Excel файл готов к скачиванию!")
                
            except Exception as e:
                st.error(f"❌ Ошибка при создании Excel файла: {e}")
    
    with col2:
        if st.button("📋 Скопировать список WB SKU", type="secondary"):
            if result.wb_skus:
                wb_skus_text = '\n'.join(str(sku) for sku in result.wb_skus)
                st.text_area(
                    "Список WB SKU (скопируйте):",
                    value=wb_skus_text,
                    height=200
                )
            else:
                st.info("📭 Нет WB SKU для копирования")

# Справочная информация
with st.expander("ℹ️ Справка по алгоритму актуальных штрихкодов"):
    st.markdown("""
    ### 🔍 Как работает алгоритм поиска актуальных штрихкодов
    
    **Проблема:** У одного товара может быть несколько штрихкодов, но актуальным является только один.
    
    **Алгоритм для Wildberries:**
    - Штрихкоды хранятся в поле `wb_barcodes` как строка через `;`
    - **Актуальный штрихкод = первый в списке**
    - Пример: `"123;456;789"` → актуальный `"123"`
    
    **Алгоритм для Ozon:**
    - Штрихкоды хранятся в отдельной таблице `oz_barcodes`
    - **Актуальный штрихкод = последний по порядку добавления в базу (ROWID)**
    - Пример: штрихкоды в порядке добавления `["4811566457476", "4815550273875", "4815694566628"]` → актуальный `"4815694566628"`
    
    **Логика сопоставления:**
    - Актуальный штрихкод OZ ищется среди **всех штрихкодов WB товаров** (не только актуальных)
    - Это позволяет найти связи даже если актуальные штрихкоды на разных МП не совпадают
    
    **Преимущества алгоритма:**
    - ✅ Находит больше корректных связей между товарами
    - ✅ Избегает потери связей из-за разных алгоритмов обновления штрихкодов
    - ✅ Обеспечивает актуальность товаров OZ при поиске
    
    **Результат:** Максимальное покрытие связями при сохранении точности сопоставления.
    """)

# Информация о состоянии базы данных
with st.expander("🔧 Информация о базе данных"):
    st.markdown("### 📊 Статистика таблиц")
    
    try:
        # Статистика по таблицам
        tables_stats = []
        
        # oz_products
        oz_products_count = db_conn.execute("SELECT COUNT(*) FROM oz_products").fetchone()[0]
        tables_stats.append({"Таблица": "oz_products", "Записей": oz_products_count})
        
        # oz_barcodes  
        oz_barcodes_count = db_conn.execute("SELECT COUNT(*) FROM oz_barcodes").fetchone()[0]
        tables_stats.append({"Таблица": "oz_barcodes", "Записей": oz_barcodes_count})
        
        # wb_products
        wb_products_count = db_conn.execute("SELECT COUNT(*) FROM wb_products").fetchone()[0]
        tables_stats.append({"Таблица": "wb_products", "Записей": wb_products_count})
        
        stats_df = pd.DataFrame(tables_stats)
        st.dataframe(stats_df, use_container_width=True)
        
    except Exception as e:
        st.error(f"Ошибка при получении статистики БД: {e}") 