"""
Streamlit page for WB Recommendations - создание рекомендаций для WB товаров.

Эта страница предоставляет функционал для:
- Пакетной обработки WB SKU для поиска рекомендаций
- Конфигурации алгоритма рекомендаций
- Экспорта результатов в таблицу Excel/CSV
- Статистики и аналитики процесса

Принцип работы:
1. Пользователь загружает список WB SKU
2. Система обогащает WB товары данными из Ozon через штрихкоды
3. Применяет адаптированный алгоритм рекомендаций
4. Формирует итоговую таблицу с рекомендациями
"""

import streamlit as st
import pandas as pd
import time
import io
import logging
from typing import List, Dict, Any, Optional

# Настройка логгирования для Streamlit
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('wb_recommendations.log')
    ]
)
logger = logging.getLogger(__name__)

# Импорты наших модулей
from utils.db_connection import connect_db
from utils.wb_recommendations import (
    WBRecommendationProcessor, WBScoringConfig, WBProcessingStatus,
    WBProcessingResult, WBBatchResult
)

# Конфигурация страницы
st.set_page_config(
    page_title="🎯 WB Рекомендации", 
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🎯 WB Рекомендации - Поиск похожих товаров")
st.markdown("---")

# Добавляем debug информацию
if st.checkbox("🐛 Debug режим", value=False):
    st.subheader("🔍 Debug информация")
    debug_container = st.container()
    
    with debug_container:
        st.write("**Состояние приложения:**")
        st.json({
            "session_state_keys": list(st.session_state.keys()),
            "wb_recommendation_processor": st.session_state.get('wb_recommendation_processor') is not None,
            "wb_batch_result": st.session_state.get('wb_batch_result') is not None,
            "wb_skus_input": len(st.session_state.get('wb_skus_input', ''))
        })

# --- Подключение к базе данных ---
@st.cache_resource
def get_database_connection():
    """Кэшированное подключение к БД"""
    logger.info("🔌 Подключение к базе данных...")
    try:
        conn = connect_db()
        logger.info("✅ База данных подключена")
        return conn
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к БД: {e}")
        return None

logger.info("🚀 Загрузка страницы WB Рекомендации")

with st.spinner("🔌 Подключение к базе данных..."):
    conn = get_database_connection()

if not conn:
    st.error("❌ База данных не подключена. Пожалуйста, настройте подключение в настройках.")
    if st.button("🔧 Перейти к настройкам"):
        st.switch_page("pages/3_⚙️_Настройки.py")
    st.stop()
else:
    st.success("✅ База данных подключена")

# --- Инициализация состояния ---
if 'wb_recommendation_processor' not in st.session_state:
    st.session_state.wb_recommendation_processor = None
    logger.info("📝 Инициализация wb_recommendation_processor")

if 'wb_batch_result' not in st.session_state:
    st.session_state.wb_batch_result = None
    logger.info("📝 Инициализация wb_batch_result")

if 'wb_skus_input' not in st.session_state:
    st.session_state.wb_skus_input = ""
    logger.info("📝 Инициализация wb_skus_input")

# --- Функции для UI ---
def render_scoring_config_ui() -> WBScoringConfig:
    """Отрисовка UI для конфигурации алгоритма"""
    logger.info("⚙️ Отрисовка UI конфигурации алгоритма")
    st.subheader("⚙️ Конфигурация алгоритма")
    
    # Выбор пресета
    preset = st.selectbox(
        "Выберите пресет:",
        ["balanced", "size_focused", "price_focused", "quality_focused", "conservative"],
        format_func=lambda x: {
            "balanced": "⚖️ Сбалансированный",
            "size_focused": "📏 Фокус на размере",
            "price_focused": "💰 Фокус на цене",
            "quality_focused": "⭐ Фокус на качестве",
            "conservative": "🔒 Консервативный"
        }[x],
        help="Выберите предустановленную конфигурацию алгоритма"
    )
    
    logger.info(f"📊 Выбран пресет: {preset}")
    
    # Получаем базовую конфигурацию
    try:
        config = WBScoringConfig.get_preset(preset)
        logger.info(f"✅ Конфигурация загружена: {preset}")
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки конфигурации: {e}")
        st.error(f"❌ Ошибка загрузки конфигурации: {e}")
        config = WBScoringConfig()
    
    # Дополнительные настройки
    with st.expander("🔧 Дополнительные настройки"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Основные параметры:**")
            config.max_recommendations = st.slider(
                "Максимальное количество рекомендаций:",
                min_value=5, max_value=50, value=config.max_recommendations,
                help="Максимальное количество рекомендаций на один товар (рекомендуется 20)"
            )
            
            config.min_recommendations = st.slider(
                "Минимальное количество рекомендаций:",
                min_value=1, max_value=25, value=config.min_recommendations,
                help="Минимальное количество рекомендаций для предупреждения (рекомендуется 5-10)"
            )
            
            config.min_score_threshold = st.slider(
                "Минимальный порог score:",
                min_value=0.0, max_value=200.0, value=config.min_score_threshold,
                help="Минимальный score для включения в рекомендации"
            )
        
        with col2:
            st.write("**Веса параметров:**")
            config.exact_size_weight = st.slider(
                "Точный размер:", 
                min_value=0, max_value=200, value=config.exact_size_weight
            )
            
            config.season_match_bonus = st.slider(
                "Совпадение сезона:", 
                min_value=0, max_value=150, value=config.season_match_bonus
            )
            
            config.price_similarity_bonus = st.slider(
                "Схожесть цены:", 
                min_value=0, max_value=50, value=config.price_similarity_bonus
            )
    
    logger.info(f"📋 Конфигурация готова: max_rec={config.max_recommendations}, min_rec={config.min_recommendations}")
    return config

def parse_wb_skus(input_text: str) -> List[str]:
    """Парсинг списка WB SKU из текста"""
    logger.info(f"📝 Парсинг WB SKU из текста длиной {len(input_text)} символов")
    
    if not input_text.strip():
        logger.info("⚠️ Пустой текст для парсинга")
        return []
    
    # Разделяем по переносам строк, запятым, пробелам
    import re
    skus = re.split(r'[\n,;\s]+', input_text.strip())
    
    # Фильтруем и очищаем
    cleaned_skus = []
    for sku in skus:
        sku = sku.strip()
        if sku and sku.isdigit():
            cleaned_skus.append(sku)
    
    logger.info(f"✅ Найдено {len(cleaned_skus)} валидных WB SKU")
    return cleaned_skus

def render_wb_skus_input() -> List[str]:
    """Отрисовка UI для ввода WB SKU"""
    logger.info("📝 Отрисовка UI для ввода WB SKU")
    st.subheader("📝 Ввод WB SKU")
    
    # Способы ввода
    input_method = st.radio(
        "Способ ввода:",
        ["text", "file"],
        format_func=lambda x: {
            "text": "📝 Текстовый ввод",
            "file": "📄 Загрузка файла"
        }[x]
    )
    
    wb_skus = []
    
    if input_method == "text":
        # Текстовый ввод
        input_text = st.text_area(
            "Введите WB SKU (по одному на строку или через запятую):",
            value=st.session_state.wb_skus_input,
            height=200,
            placeholder="Например:\n123456789\n987654321\n555666777",
            help="Вводите WB SKU по одному на строку или через запятую"
        )
        
        st.session_state.wb_skus_input = input_text
        wb_skus = parse_wb_skus(input_text)
        
    else:
        # Загрузка файла
        uploaded_file = st.file_uploader(
            "Загрузите файл с WB SKU:",
            type=['txt', 'csv', 'xlsx'],
            help="Поддерживаются файлы: TXT, CSV, XLSX"
        )
        
        if uploaded_file:
            logger.info(f"📄 Загружен файл: {uploaded_file.name}, тип: {uploaded_file.type}")
            try:
                if uploaded_file.type == "text/plain":
                    # TXT файл
                    content = uploaded_file.read().decode('utf-8')
                    wb_skus = parse_wb_skus(content)
                
                elif uploaded_file.type == "text/csv":
                    # CSV файл
                    df = pd.read_csv(uploaded_file)
                    # Берем первый столбец
                    wb_skus = df.iloc[:, 0].astype(str).tolist()
                
                elif uploaded_file.type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
                    # XLSX файл
                    df = pd.read_excel(uploaded_file)
                    # Берем первый столбец
                    wb_skus = df.iloc[:, 0].astype(str).tolist()
                
                # Очищаем список
                wb_skus = [sku.strip() for sku in wb_skus if sku.strip().isdigit()]
                
                logger.info(f"✅ Успешно загружено {len(wb_skus)} WB SKU из файла")
                st.success(f"✅ Загружено {len(wb_skus)} WB SKU из файла")
                
            except Exception as e:
                logger.error(f"❌ Ошибка загрузки файла: {e}")
                st.error(f"❌ Ошибка загрузки файла: {e}")
                wb_skus = []
    
    # Показываем информацию о введенных SKU
    if wb_skus:
        st.info(f"📊 Найдено {len(wb_skus)} валидных WB SKU")
        logger.info(f"📊 Пользователь ввел {len(wb_skus)} WB SKU")
        
        # Показываем первые несколько для проверки
        if len(wb_skus) <= 10:
            st.write("**Список WB SKU:**")
            for i, sku in enumerate(wb_skus, 1):
                st.write(f"{i}. {sku}")
        else:
            st.write("**Превью первых 10 WB SKU:**")
            for i, sku in enumerate(wb_skus[:10], 1):
                st.write(f"{i}. {sku}")
            st.write(f"... и еще {len(wb_skus) - 10} SKU")
    
    return wb_skus

def render_batch_results(batch_result: WBBatchResult):
    """Отрисовка результатов пакетной обработки"""
    logger.info("📊 Отрисовка результатов пакетной обработки")
    st.subheader("📊 Результаты пакетной обработки")
    
    # Общая статистика
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Всего обработано", len(batch_result.processed_items))
    
    with col2:
        st.metric("Успешно", batch_result.success_count)
    
    with col3:
        st.metric("Ошибок", batch_result.error_count)
    
    with col4:
        st.metric("Успешность", f"{batch_result.success_rate:.1f}%")
    
    # Время обработки
    st.info(f"⏱️ Общее время обработки: {batch_result.total_processing_time:.1f} секунд")
    
    # Детализированная таблица результатов
    st.subheader("📋 Детальные результаты")
    
    # Подготовка данных для таблицы
    results_data = []
    for result in batch_result.processed_items:
        results_data.append({
            "WB SKU": result.wb_sku,
            "Статус": result.status.value,
            "Количество рекомендаций": len(result.recommendations),
            "Время обработки (с)": f"{result.processing_time:.2f}",
            "Ошибка": result.error_message or ""
        })
    
    results_df = pd.DataFrame(results_data)
    
    # Фильтрация по статусу
    status_filter = st.selectbox(
        "Фильтр по статусу:",
        ["Все"] + [status.value for status in WBProcessingStatus],
        format_func=lambda x: {
            "Все": "🔍 Все результаты",
            "success": "✅ Успешные",
            "no_similar": "⚠️ Нет похожих",
            "insufficient_recommendations": "📉 Мало рекомендаций",
            "error": "❌ Ошибки",
            "no_data": "📭 Нет данных",
            "no_ozon_link": "🔗 Нет связи с Ozon"
        }.get(x, x)
    )
    
    if status_filter != "Все":
        filtered_df = results_df[results_df["Статус"] == status_filter]
    else:
        filtered_df = results_df
    
    st.dataframe(filtered_df, use_container_width=True)
    
    # Кнопка экспорта детальных результатов
    if st.button("📥 Экспорт детальных результатов"):
        csv = results_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Скачать CSV",
            data=csv,
            file_name=f"wb_recommendations_detailed_{int(time.time())}.csv",
            mime="text/csv"
        )

def create_recommendations_table(batch_result: WBBatchResult) -> pd.DataFrame:
    """Создание итоговой таблицы рекомендаций"""
    logger.info("📊 Создание итоговой таблицы рекомендаций")
    table_data = []
    
    for result in batch_result.processed_items:
        if result.success and result.recommendations:
            wb_sku = result.wb_sku
            
            # Добавляем каждую рекомендацию как отдельную строку
            for i, recommendation in enumerate(result.recommendations, 1):
                table_data.append({
                    "Исходный WB SKU": wb_sku,
                    "Рекомендация №": i,
                    "Рекомендуемый WB SKU": recommendation.product_info.wb_sku,
                    "Score": round(recommendation.score, 1),
                    "Бренд": recommendation.product_info.wb_brand,
                    "Категория": recommendation.product_info.wb_category,
                    "Размеры": recommendation.product_info.get_size_range_str(),
                    "Остаток": recommendation.product_info.wb_fbo_stock,
                    "Цена": recommendation.product_info.wb_full_price,
                    "Тип": recommendation.product_info.enriched_type,
                    "Пол": recommendation.product_info.enriched_gender,
                    "Сезон": recommendation.product_info.enriched_season,
                    "Цвет": recommendation.product_info.enriched_color,
                    "Материал": recommendation.product_info.punta_material_short,
                    "Колодка MEGA": recommendation.product_info.punta_mega_last,
                    "Колодка BEST": recommendation.product_info.punta_best_last,
                    "Колодка NEW": recommendation.product_info.punta_new_last,
                    "Качество обогащения": f"{recommendation.product_info.get_enrichment_score():.1%}",
                    "Источник обогащения": recommendation.product_info.enrichment_source,
                    "Детали совпадения": recommendation.match_details.split('\n')[0] if recommendation.match_details else ""
                })
    
    logger.info(f"📋 Создана таблица с {len(table_data)} рекомендациями")
    return pd.DataFrame(table_data)

# --- Основной интерфейс ---
logger.info("🎛️ Отрисовка основного интерфейса")
st.subheader("🎛️ Управление")

# Настройка алгоритма
try:
    config = render_scoring_config_ui()
    logger.info("✅ Конфигурация алгоритма готова")
except Exception as e:
    logger.error(f"❌ Ошибка конфигурации алгоритма: {e}")
    st.error(f"❌ Ошибка конфигурации алгоритма: {e}")
    st.stop()

# Создание процессора
if st.button("🔄 Обновить процессор"):
    logger.info("🔄 Обновление процессора...")
    try:
        with st.spinner("🔄 Создание процессора..."):
            st.session_state.wb_recommendation_processor = WBRecommendationProcessor(conn, config)
        logger.info("✅ Процессор обновлен")
        st.success("✅ Процессор обновлен!")
    except Exception as e:
        logger.error(f"❌ Ошибка создания процессора: {e}")
        st.error(f"❌ Ошибка создания процессора: {e}")

# Создание процессора если не существует
if st.session_state.wb_recommendation_processor is None:
    logger.info("🔄 Создание процессора (первый раз)...")
    try:
        with st.spinner("🔄 Инициализация процессора..."):
            st.session_state.wb_recommendation_processor = WBRecommendationProcessor(conn, config)
        logger.info("✅ Процессор создан")
    except Exception as e:
        logger.error(f"❌ Ошибка создания процессора: {e}")
        st.error(f"❌ Ошибка создания процессора: {e}")
        st.stop()

processor = st.session_state.wb_recommendation_processor

st.markdown("---")

# Ввод WB SKU
logger.info("📝 Отрисовка ввода WB SKU")
wb_skus = render_wb_skus_input()

# Обработка
if wb_skus:
    st.markdown("---")
    st.subheader("🚀 Обработка")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        if st.button("🚀 Запустить пакетную обработку", type="primary", use_container_width=True):
            logger.info(f"🚀 Запуск пакетной обработки для {len(wb_skus)} WB SKU")
            
            # Контейнер для прогресса
            progress_container = st.container()
            
            with progress_container:
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # Callback для обновления прогресса
                def progress_callback(current: int, total: int, message: str):
                    progress = current / total
                    progress_bar.progress(progress)
                    status_text.text(f"Обработано {current}/{total}: {message}")
                    logger.info(f"📊 Прогресс: {current}/{total} - {message}")
                
                start_time = time.time()
                
                # Пакетная обработка с автоматическим выбором алгоритма
                try:
                    # Определяем тип обработки по размеру пакета
                    if len(wb_skus) >= 50:
                        logger.info(f"🚀 Запуск ОПТИМИЗИРОВАННОЙ пакетной обработки для {len(wb_skus)} товаров...")
                        st.info(f"🚀 Обнаружен большой пакет ({len(wb_skus)} товаров) - используем оптимизированный алгоритм")
                        batch_result = processor.process_batch_optimized(wb_skus, progress_callback)
                    else:
                        logger.info(f"🔄 Запуск стандартной пакетной обработки для {len(wb_skus)} товаров...")
                        batch_result = processor.process_batch(wb_skus, progress_callback)
                    
                    processing_time = time.time() - start_time
                    st.session_state.wb_batch_result = batch_result
                    
                    # Расчет ускорения для больших пакетов
                    if len(wb_skus) >= 50:
                        estimated_old_time = len(wb_skus) * 3  # Примерно 3 секунды на товар
                        speedup = estimated_old_time / processing_time
                        logger.info(f"✅ ОПТИМИЗИРОВАННАЯ обработка завершена за {processing_time:.1f}с (ускорение: ~{speedup:.1f}x)")
                        st.success(f"🚀 Оптимизированная обработка завершена за {processing_time:.1f}с! Ускорение: ~{speedup:.1f}x")
                        st.info(f"💡 Без оптимизации потребовалось бы ~{estimated_old_time/60:.1f} минут")
                    else:
                        logger.info(f"✅ Пакетная обработка завершена за {processing_time:.1f}с")
                        st.success(f"✅ Пакетная обработка завершена за {processing_time:.1f}с!")
                    
                    st.rerun()
                    
                except Exception as e:
                    processing_time = time.time() - start_time
                    logger.error(f"❌ Ошибка пакетной обработки: {e}")
                    st.error(f"❌ Ошибка пакетной обработки: {e}")
    
    with col2:
        st.info(f"**Готово к обработке:** {len(wb_skus)} WB SKU")
        
        # Очистка результатов
        if st.button("🧹 Очистить результаты", use_container_width=True):
            logger.info("🧹 Очистка результатов")
            st.session_state.wb_batch_result = None
            st.rerun()

# Результаты
if st.session_state.wb_batch_result:
    st.markdown("---")
    render_batch_results(st.session_state.wb_batch_result)
    
    # Создание и экспорт итоговой таблицы
    st.markdown("---")
    st.subheader("📊 Итоговая таблица рекомендаций")
    
    recommendations_df = create_recommendations_table(st.session_state.wb_batch_result)
    
    if not recommendations_df.empty:
        st.info(f"📊 Создана таблица с {len(recommendations_df)} рекомендациями")
        
        # Показываем превью
        st.dataframe(recommendations_df.head(20), use_container_width=True)
        
        if len(recommendations_df) > 20:
            st.info(f"Показаны первые 20 строк из {len(recommendations_df)}")
        
        # Экспорт
        col1, col2 = st.columns(2)
        
        with col1:
            # CSV экспорт
            csv = recommendations_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Скачать CSV",
                data=csv,
                file_name=f"wb_recommendations_{int(time.time())}.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        with col2:
            # Excel экспорт
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                recommendations_df.to_excel(writer, index=False, sheet_name='Рекомендации')
            
            st.download_button(
                label="📥 Скачать Excel",
                data=output.getvalue(),
                file_name=f"wb_recommendations_{int(time.time())}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
    else:
        st.warning("⚠️ Нет успешных рекомендаций для экспорта")

# Статистика
st.markdown("---")
st.subheader("📊 Статистика системы")

try:
    logger.info("📊 Загрузка статистики...")
    stats_start = time.time()
    
    with st.spinner("📊 Загрузка статистики..."):
        stats = processor.get_statistics()
    
    stats_time = time.time() - stats_start
    logger.info(f"✅ Статистика загружена за {stats_time:.2f}с")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Всего WB товаров", stats.get('total_wb_products', 0))
    
    with col2:
        st.metric("WB товаров в наличии", stats.get('wb_products_in_stock', 0))
    
    with col3:
        st.metric("Связанные с Ozon", stats.get('wb_products_linked_to_ozon', 0))
    
    with col4:
        st.metric("Покрытие связывания", f"{stats.get('linking_coverage', 0):.1f}%")

except Exception as e:
    logger.error(f"❌ Ошибка загрузки статистики: {e}")
    st.error(f"❌ Ошибка загрузки статистики: {e}")
    st.info("💡 Возможно, проблема с SQL запросами. Проверьте логи для диагностики.")

# --- Справка в сайдбаре ---
with st.sidebar.expander("❓ Справка", expanded=False):
    st.markdown("""
    **🎯 Алгоритм работы:**
    1. Обогащение WB товаров данными из Ozon через штрихкоды
    2. Поиск похожих WB товаров по характеристикам
    3. Оценка similarity score по множеству параметров
    4. Формирование рекомендаций с детализацией
    
    **🚀 Оптимизация производительности:**
    - **Малые пакеты (<50 товаров)**: стандартный алгоритм
    - **Большие пакеты (≥50 товаров)**: оптимизированный алгоритм
    - **Ускорение**: в 5-20 раз быстрее для больших пакетов
    - **920 товаров**: ~5-10 минут вместо часов
    
    **📋 Требования:**
    - WB товары должны быть связаны с Ozon товарами
    - Наличие характеристик в Ozon (тип, пол, бренд)
    - Остатки на складе WB > 0
    
    **⚙️ Настройки:**
    - Количество рекомендаций: 5-50
    - Минимальный score: 0-200
    - Веса параметров: размер, сезон, цена, качество
    
    **📊 Экспорт:**
    - CSV: для анализа в Excel
    - Excel: готовая таблица с форматированием
    """)

# --- Логи в сайдбаре ---
with st.sidebar.expander("📝 Логи", expanded=False):
    st.markdown("""
    **📝 Логи сохраняются в файл:**
    - `wb_recommendations.log` - основной лог приложения
    - `streamlit.log` - логи Streamlit
    
    **🔍 Для отладки:**
    - Включите Debug режим в начале страницы
    - Посмотрите логи в консоли
    - Проверьте состояние сессии
    """)
    
    # Показать последние строки лога
    try:
        with open('wb_recommendations.log', 'r') as f:
            lines = f.readlines()
        last_lines = lines[-10:] if len(lines) > 10 else lines
        st.code('\n'.join(last_lines), language='text')
    except:
        st.info("Лог файл не найден")

# --- Footer ---
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: #666;'>"
    "🎯 WB Recommendations Engine • Powered by Cross-Platform Analytics"
    "</div>", 
    unsafe_allow_html=True
)

logger.info("✅ Загрузка страницы завершена") 