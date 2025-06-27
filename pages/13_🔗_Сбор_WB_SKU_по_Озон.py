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
from utils.db_connection import get_connection_and_ensure_schema
from utils.oz_to_wb_collector import (
    OzToWbCollector, 
    WbSkuCollectionResult,
    collect_wb_skus_for_all_oz_products,
    collect_wb_skus_for_oz_list
)

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
        st.info("📊 Будет обработан весь ассортимент из таблицы oz_products")
    else:
        if oz_skus_input:
            st.success(f"📝 Готово к обработке {len(oz_skus_input)} oz_sku")
        else:
            st.warning("⚠️ Укажите oz_sku для обработки")

with col2:
    st.header("ℹ️ Алгоритм")
    st.markdown("""
    **Актуальные штрихкоды:**
    - **WB**: последний в списке через `;`
    - **OZ**: последний в алфавитном порядке
    
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
    
    # Запуск сбора
    with st.spinner("🔄 Сбор wb_sku в процессе..."):
        try:
            if collection_mode == "🔄 Весь ассортимент Озон":
                result = collect_wb_skus_for_all_oz_products(db_conn)
            else:
                result = collect_wb_skus_for_oz_list(db_conn, oz_skus_input)
            
            # Сохраняем результат в сессии
            st.session_state.collection_result = result
            st.session_state.collection_timestamp = datetime.now()
            
        except Exception as e:
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
        st.metric(
            "✅ OZ SKU со связями",
            result.stats['oz_skus_with_wb_links'],
            help="Количество oz_sku для которых найдены соответствующие wb_sku"
        )
    
    with col3:
        st.metric(
            "❌ OZ SKU без связей",
            result.stats['oz_skus_without_wb_links'],
            help="Количество oz_sku для которых НЕ найдены wb_sku"
        )
    
    with col4:
        st.metric(
            "🔀 Дубликаты связей",
            result.stats['duplicate_mappings_count'],
            help="Количество oz_sku связанных с несколькими wb_sku"
        )
    
    # Детальные результаты
    st.header("📋 Детальные результаты")
    
    # Вкладки для разных типов результатов
    tab1, tab2, tab3, tab4 = st.tabs([
        "🎯 Найденные WB SKU", 
        "❌ OZ SKU без связей", 
        "🔀 Дубликаты связей",
        "📊 Полная статистика"
    ])
    
    with tab1:
        st.subheader("🎯 Найденные WB SKU")
        
        if result.wb_skus:
            # Создаем DataFrame для отображения
            wb_skus_df = pd.DataFrame({
                'wb_sku': result.wb_skus
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
                'oz_sku': result.no_links_oz_skus
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
            {'Метрика': 'Время обработки (сек)', 'Значение': f"{result.stats['processing_time_seconds']:.3f}"},
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
                            'wb_sku': result.wb_skus
                        })
                        wb_skus_df.to_excel(
                            writer, 
                            sheet_name='Found_WB_SKUs', 
                            index=False
                        )
                    
                    # Лист 2: OZ SKU без связей с WB
                    if result.no_links_oz_skus:
                        pd.DataFrame({
                            'oz_sku_without_wb_links': result.no_links_oz_skus
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
                wb_skus_text = '\n'.join(result.wb_skus)
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
    - **Актуальный штрихкод = последний в списке**
    - Пример: `"123;456;789"` → актуальный `"789"`
    
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