"""
Rich Content OZ v2 - Упрощенная версия для пакетной обработки товаров Ozon.

Основные принципы:
- Минимализм - только необходимый функционал
- Модульность - избегаем файлов 2000+ строк  
- CSV-ориентированность - экспорт вместо сохранения в БД
- Переиспользование - существующий ScoringConfig и алгоритмы
- Расширяемость - легкая модификация алгоритма в будущем
"""

import streamlit as st
import pandas as pd
import time
import logging
from typing import List, Optional

# Импорты наших модулей
from utils.db_connection import connect_db
from utils.rich_content_oz import ScoringConfig, RichContentProcessor
from utils.rich_content_processor_v2 import BatchProcessorV2
from utils.csv_exporter import RichContentCSVExporter

# Настройка логирования
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Конфигурация страницы
st.set_page_config(
    page_title="⚡ Rich Content OZ v2", 
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("⚡ Rich Content OZ v2 - Упрощенная пакетная обработка")
st.markdown("---")

# --- Подключение к базе данных ---
@st.cache_resource
def get_database_connection():
    """Кэшированное подключение к БД"""
    return connect_db()

conn = get_database_connection()
if not conn:
    st.error("❌ База данных не подключена. Пожалуйста, настройте подключение в настройках.")
    if st.button("🔧 Перейти к настройкам"):
        st.switch_page("pages/3_⚙️_Настройки.py")
    st.stop()

# --- Функции обработки ---

def parse_vendor_codes(input_text: str) -> List[str]:
    """Парсинг артикулов из текстового ввода"""
    if not input_text.strip():
        return []
    
    # Разбиваем по строкам и очищаем
    lines = input_text.strip().split('\n')
    vendor_codes = []
    
    for line in lines:
        code = line.strip()
        if code and len(code) > 2:  # Базовая валидация
            vendor_codes.append(code)
    
    return vendor_codes

def process_batch_v2(input_text: str, config: ScoringConfig):
    """Основная логика пакетной обработки v2"""
    
    # Парсинг артикулов
    vendor_codes = parse_vendor_codes(input_text)
    
    if not vendor_codes:
        st.error("❌ Не найдено валидных артикулов для обработки")
        return
    
    logger.info(f"Начинаем обработку {len(vendor_codes)} артикулов")
    
    # Создаем процессор
    if st.session_state.batch_processor is None:
        st.session_state.batch_processor = BatchProcessorV2(conn, config)
    
    processor = st.session_state.batch_processor
    
    # Контейнеры для прогресса
    progress_container = st.container()
    log_container = st.container()
    
    with progress_container:
        progress_bar = st.progress(0)
        status_text = st.empty()
        
    with log_container:
        st.subheader("📋 Лог обработки")
        log_placeholder = st.empty()
    
    try:
        start_time = time.time()
        
        # Callback для обновления прогресса
        def progress_callback(current: int, total: int, message: str):
            progress = current / total if total > 0 else 0
            progress_bar.progress(progress)
            status_text.text(f"Обработано {current}/{total}: {message}")
            
            # Обновляем лог
            elapsed = time.time() - start_time
            avg_time = elapsed / current if current > 0 else 0
            estimated_remaining = avg_time * (total - current) if current > 0 else 0
            
            log_text = f"""
⏱️ Время: {elapsed:.1f}с | Осталось: ~{estimated_remaining:.1f}с
📊 Прогресс: {current}/{total} ({progress*100:.1f}%)
🔄 Текущий: {message}
⚡ Средняя скорость: {avg_time:.2f}с/товар
            """
            log_placeholder.text(log_text)
        
        # Обработка
        logger.info(f"Запускаем пакетную обработку {len(vendor_codes)} товаров")
        
        results = processor.process_vendor_codes_list(
            vendor_codes, 
            progress_callback=progress_callback
        )
        
        # Завершение
        total_time = time.time() - start_time
        progress_bar.progress(1.0)
        status_text.text(f"✅ Обработка завершена за {total_time:.1f}с!")
        
        # Сохраняем результаты
        st.session_state.processing_results = results
        
        logger.info(f"Обработка завершена: {len(results)} результатов за {total_time:.1f}с")
        
        # Небольшая пауза и перезагрузка для отображения результатов
        time.sleep(1)
        st.rerun()
        
    except Exception as e:
        progress_bar.progress(1.0)
        status_text.text("❌ Ошибка обработки!")
        log_placeholder.text(f"❌ Критическая ошибка: {str(e)}")
        st.error(f"Ошибка обработки: {e}")
        logger.error(f"Критическая ошибка обработки: {e}")

# --- Инициализация состояния ---
if 'processing_results' not in st.session_state:
    st.session_state.processing_results = None

if 'batch_processor' not in st.session_state:
    st.session_state.batch_processor = None

# --- Основной интерфейс ---

# 1. Ввод артикулов
st.subheader("📝 Ввод данных")
vendor_codes_input = st.text_area(
    "**Список артикулов товаров (по одному на строку):**",
    height=200,
    placeholder="Например:\nABC-123-XL\nDEF-456-M\nGHI-789-L\n...",
    help="Введите артикулы товаров из таблицы oz_category_products, каждый на новой строке"
)

# 2. Конфигурация алгоритма (свернутая по умолчанию)
with st.expander("⚙️ Настройки алгоритма рекомендаций", expanded=False):
    st.markdown("**Система весов для оценки схожести товаров:**")
    
    # Выбор предустановленной конфигурации
    col1, col2 = st.columns([1, 1])
    
    with col1:
        config_preset = st.selectbox(
            "**Предустановленная конфигурация:**",
            ["default", "optimized", "balanced", "lenient"],
            index=1,  # По умолчанию "optimized"
            help="""
            • **default** - оригинальная конфигурация
            • **optimized** - улучшенная для лучшего качества рекомендаций  
            • **balanced** - сбалансированная между качеством и строгостью
            • **lenient** - мягкая, максимально включающая
            """
        )
    
    with col2:
        if config_preset == "optimized":
            st.success("🎯 **Рекомендуется** для лучшего качества")
        elif config_preset == "balanced":
            st.info("⚖️ Сбалансированный подход")
        elif config_preset == "lenient":
            st.warning("🔓 Мягкая фильтрация")
        else:
            st.info("📊 Оригинальные настройки")
    
    # Получаем конфигурацию
    try:
        from utils.scoring_config_optimized import get_config_by_name
        config = get_config_by_name(config_preset)
        
        # Показываем ключевые параметры выбранной конфигурации
        st.markdown("**Ключевые параметры выбранной конфигурации:**")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Мин. порог", f"{config.min_score_threshold}")
        with col2:
            st.metric("Штраф за колодку", f"{int((1-config.no_last_penalty)*100)}%")
        with col3:
            st.metric("Макс. рекомендаций", config.max_recommendations)
        with col4:
            st.metric("Базовый score", config.base_score)
        
    except ImportError:
        st.warning("⚠️ Оптимизированные конфигурации недоступны, используем стандартную")
        config = render_scoring_config_ui()

# 3. Кнопка обработки
col1, col2 = st.columns([1, 1])

with col1:
    if st.button("🚀 Обработать товары", type="primary", use_container_width=True):
        process_batch_v2(vendor_codes_input, config)

with col2:
    # Показываем количество введенных артикулов
    if vendor_codes_input.strip():
        vendor_codes = parse_vendor_codes(vendor_codes_input)
        if vendor_codes:
            st.success(f"📊 Готово к обработке: **{len(vendor_codes)}** товаров")
        else:
            st.warning("⚠️ Не найдено валидных артикулов")
    else:
        st.info("👆 Введите артикулы товаров для обработки")

# --- Результаты обработки ---
if st.session_state.processing_results:
    st.markdown("---")
    st.subheader("📊 Результаты обработки")
    
    results = st.session_state.processing_results
    
    # Статистика
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Всего обработано", len(results))
    
    with col2:
        successful = len([r for r in results if r.get('rich_content_json')])
        st.metric("Успешно", successful)
    
    with col3:
        failed = len(results) - successful
        st.metric("Ошибки", failed)
    
    with col4:
        success_rate = (successful / len(results) * 100) if results else 0
        st.metric("Успешность", f"{success_rate:.1f}%")
    
    # CSV экспорт
    st.markdown("### 📥 Экспорт результатов")
    
    if successful > 0:
        exporter = RichContentCSVExporter()
        
        # Фильтруем только успешные результаты для экспорта
        successful_results = [r for r in results if r.get('rich_content_json')]
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.success(f"✅ Готово к экспорту: **{len(successful_results)}** товаров с Rich Content")
        
        with col2:
            exporter.create_download_button(
                successful_results, 
                filename=f"rich_content_export_{int(time.time())}.csv"
            )
        
        # Превью данных
        with st.expander("👁️ Превью экспортируемых данных", expanded=False):
            preview_df = pd.DataFrame([
                {
                    'oz_vendor_code': r['oz_vendor_code'],
                    'recommendations_count': r.get('recommendations_count', 0),
                    'processing_time_ms': round(r.get('processing_time', 0) * 1000, 1),
                    'has_rich_content': bool(r.get('rich_content_json'))
                }
                for r in successful_results[:20]  # Показываем первые 20
            ])
            
            st.dataframe(preview_df, use_container_width=True)
            
            if len(successful_results) > 20:
                st.caption(f"Показаны первые 20 из {len(successful_results)} записей. Rich Content JSON включен в полный экспорт.")
    else:
        st.warning("⚠️ Нет успешных результатов для экспорта")

# --- Справка в сайдбаре ---
with st.sidebar.expander("❓ Справка", expanded=False):
    st.markdown("""
    **⚡ Rich Content OZ v2:**
    
    **Упрощенная версия** для быстрой пакетной обработки товаров.
    
    **Как использовать:**
    1. Введите список артикулов (по одному на строку)
    2. При необходимости настройте веса алгоритма
    3. Нажмите "Обработать товары"
    4. Скачайте результаты в CSV формате
    
    **Формат CSV:**
    - Первый столбец: `oz_vendor_code` (артикул)
    - Второй столбец: `rich_content_json` (JSON контент)
    
    **Алгоритм рекомендаций:**
    - Обязательные критерии: тип, пол, бренд, размер
    - Дополнительные: сезон, цвет, материал, колодки
    - Учитывает остатки на складе
    
    **Производительность:**
    - Оптимизированная пакетная обработка
    - ~0.2-1 секунда на товар
    - Поддержка больших объемов данных
    """)

# --- Footer ---
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: #666;'>"
    "⚡ Rich Content Generator v2 для Ozon • Упрощенная и быстрая версия"
    "</div>", 
    unsafe_allow_html=True
)
