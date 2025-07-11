"""
Streamlit page for OZ Rich Content - создание rich-контента для карточек на Озоне.

Эта страница предоставляет функционал для:
- Генерации рекомендаций похожих товаров
- Создания Rich Content JSON для Ozon
- Пакетной обработки товаров
- Конфигурации алгоритма рекомендаций
- Превью и валидации сгенерированного контента
"""
import streamlit as st
import pandas as pd
import time
from typing import Optional, List

# Импорты наших модулей
from utils.db_connection import connect_db
from utils.config_utils import get_db_path
from utils.rich_content_oz import (
    RichContentProcessor, ScoringConfig, ProcessingResult, 
    BatchResult, ProcessingStatus
)
from utils.rich_content_oz_ui import (
    render_scoring_config_ui, render_product_selector,
    render_rich_content_preview, render_processing_results,
    render_batch_results, render_processing_statistics
)

# Импорты для захвата логов
import logging
import io
import contextlib

# Функция для захвата логов
@contextlib.contextmanager
def capture_logs():
    """Контекстный менеджер для захвата логов в Streamlit"""
    # Создаем StringIO буфер для захвата логов
    log_buffer = io.StringIO()
    
    # Создаем новый handler для захвата
    handler = logging.StreamHandler(log_buffer)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    
    # Получаем логгер rich_content_oz
    logger = logging.getLogger('utils.rich_content_oz')
    logger.addHandler(handler)
    
    try:
        yield log_buffer
    finally:
        # Удаляем handler после использования
        logger.removeHandler(handler)

# Конфигурация страницы
st.set_page_config(
    page_title="✨ OZ Rich Content", 
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("✨ OZ Rich Content - Генератор рекомендаций для Ozon")
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

# --- Инициализация состояния ---
if 'rich_content_processor' not in st.session_state:
    st.session_state.rich_content_processor = None

if 'last_single_result' not in st.session_state:
    st.session_state.last_single_result = None

if 'last_batch_result' not in st.session_state:
    st.session_state.last_batch_result = None

# --- Главное меню ---
st.subheader("🎛️ Управление")

# Создаем колонки для управления
control_col1, control_col2 = st.columns([1, 2])

with control_col1:
    # Выбор режима работы
    mode = st.radio(
        "**Режим работы:**",
        ["single", "batch", "statistics"],
        format_func=lambda x: {
            "single": "🎯 Один товар",
            "batch": "📦 Пакетная обработка", 
            "statistics": "📊 Статистика"
        }[x]
    )

with control_col2:
    # Конфигурация алгоритма
    with st.expander("⚙️ Конфигурация алгоритма", expanded=False):
        config = render_scoring_config_ui()
        
        # Инициализация процессора с новой конфигурацией
        if st.button("🔄 Обновить процессор"):
            st.session_state.rich_content_processor = RichContentProcessor(conn, config)
            st.success("Процессор обновлен!")

st.markdown("---")

# Создание процессора если не существует
if st.session_state.rich_content_processor is None:
    st.session_state.rich_content_processor = RichContentProcessor(conn, config)

processor = st.session_state.rich_content_processor

# --- Основной контент ---

if mode == "single":
    st.header("🎯 Обработка одного товара")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("📝 Ввод данных")
        
        # Ввод артикула
        vendor_code = st.text_input(
            "Артикул товара (oz_vendor_code):",
            placeholder="Например: ABC-123-XL",
            help="Введите артикул товара из таблицы oz_category_products"
        )
        
        # Выбор типа шаблона
        template_type = st.selectbox(
            "Тип шаблона Rich Content:",
            ["ozon_showcase", "recommendations_carousel", "recommendations_grid"],
            format_func=lambda x: {
                "ozon_showcase": "🏪 Витрина Ozon (рекомендуется)",
                "recommendations_carousel": "🎠 Карусель рекомендаций",
                "recommendations_grid": "📱 Сетка рекомендаций"
            }[x],
            help="Выберите формат отображения рекомендаций"
        )
        
        # Кнопка обработки
        if st.button("🚀 Создать Rich Content", type="primary", use_container_width=True):
            if not vendor_code.strip():
                st.error("Пожалуйста, введите артикул товара")
            else:
                # Создаем контейнеры для прогресса и логов
                progress_container = st.container()
                log_container = st.container()
                
                with progress_container:
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                
                with log_container:
                    st.subheader("📋 Детальный лог обработки")
                    log_placeholder = st.empty()
                
                try:
                    # Используем контекстный менеджер для захвата реальных логов
                    with capture_logs() as log_buffer:
                        
                        status_text.text("🎯 Начинаем обработку товара...")
                        progress_bar.progress(10)
                        
                        # Обработка товара (здесь будут захвачены все логи)
                        result = processor.process_single_product(vendor_code.strip())
                        
                        progress_bar.progress(90)
                        status_text.text("📝 Генерация Rich Content JSON...")
                        
                        # Обновление Rich Content JSON с нужным типом шаблона
                        if result.success and result.recommendations:
                            updated_json = processor.content_generator.generate_rich_content_json(
                                result.recommendations, 
                                template_type
                            )
                            result.rich_content_json = updated_json
                        
                        # Получаем все захваченные логи
                        captured_logs = log_buffer.getvalue()
                    
                    # Сохраняем результат
                    st.session_state.last_single_result = result
                    
                    # Отображаем логи
                    if captured_logs:
                        log_placeholder.text(captured_logs)
                    else:
                        log_placeholder.text("Логи не захвачены (возможно, логирование отключено)")
                    
                    progress_bar.progress(100)
                    if result.success:
                        status_text.text(f"✅ Обработка завершена за {result.processing_time:.2f}с!")
                    else:
                        status_text.text(f"⚠️ Обработка завершена с предупреждениями за {result.processing_time:.2f}с")
                    
                    # Небольшая пауза для показа завершения
                    time.sleep(1)
                    st.rerun()
                        
                except Exception as e:
                    progress_bar.progress(100)
                    status_text.text("❌ Ошибка обработки!")
                    log_placeholder.text(f"❌ Критическая ошибка: {str(e)}")
                    st.error(f"Ошибка обработки: {e}")
        
        # Кнопка сохранения
        if (st.session_state.last_single_result and 
            st.session_state.last_single_result.success):
            
            if st.button("💾 Сохранить в базу данных", use_container_width=True):
                with st.spinner("💾 Сохранение..."):
                    success = processor.save_rich_content_to_database(
                        st.session_state.last_single_result
                    )
                    if success:
                        st.success("✅ Rich Content успешно сохранен в БД!")
                    else:
                        st.error("❌ Ошибка сохранения в БД")
    
    with col2:
        st.subheader("📊 Результат")
        
        if st.session_state.last_single_result:
            render_processing_results(st.session_state.last_single_result)
        else:
            st.info("👆 Введите артикул и нажмите 'Создать Rich Content'")

elif mode == "batch":
    st.header("📦 Пакетная обработка товаров")
    
    # Выбор товаров
    selection_mode, selected_products = render_product_selector(conn)
    
    if selected_products:
        st.markdown("---")
        
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.subheader("🚀 Запуск обработки")
            
            # Настройки пакетной обработки
            template_type = st.selectbox(
                "Тип шаблона:",
                ["ozon_showcase", "recommendations_carousel", "recommendations_grid"],
                format_func=lambda x: {
                    "ozon_showcase": "🏪 Витрина Ozon",
                    "recommendations_carousel": "🎠 Карусель",
                    "recommendations_grid": "📱 Сетка"
                }[x]
            )
            
            auto_save = st.checkbox(
                "Автоматически сохранять в БД",
                value=True,
                help="Сохранять успешные результаты в базу данных"
            )
            
            # Кнопка запуска
            if st.button("🚀 Запустить пакетную обработку", type="primary"):
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
                    
                    start_time = time.time()
                    
                    # Пакетная обработка
                    batch_result = processor.process_batch(
                        selected_products, 
                        progress_callback
                    )
                    
                    processing_time = time.time() - start_time
                    st.session_state.last_batch_result = batch_result
                    
                    # Автосохранение
                    if auto_save:
                        saved_count = 0
                        save_progress = st.progress(0)
                        save_status = st.empty()
                        
                        for i, result in enumerate(batch_result.processed_items):
                            if result.success:
                                success = processor.save_rich_content_to_database(result)
                                if success:
                                    saved_count += 1
                            
                            save_progress.progress((i + 1) / len(batch_result.processed_items))
                            save_status.text(f"Сохранено {saved_count} из {len(batch_result.processed_items)}")
                        
                        st.success(f"✅ Пакетная обработка завершена за {processing_time:.1f}с. "
                                 f"Сохранено {saved_count} товаров.")
                    else:
                        st.success(f"✅ Пакетная обработка завершена за {processing_time:.1f}с.")
        
        with col2:
            st.subheader("ℹ️ Информация")
            st.info(f"**Выбрано товаров:** {len(selected_products)}")
            st.info(f"**Режим выбора:** {selection_mode}")
            
            if len(selected_products) <= 10:
                with st.expander("📋 Список товаров"):
                    for i, product in enumerate(selected_products, 1):
                        st.write(f"{i}. {product}")
            else:
                st.warning(f"Показаны первые 10 из {len(selected_products)} товаров:")
                with st.expander("📋 Превью товаров"):
                    for i, product in enumerate(selected_products[:10], 1):
                        st.write(f"{i}. {product}")
                    st.write(f"... и еще {len(selected_products) - 10} товаров")
    
    # Результаты пакетной обработки
    if st.session_state.last_batch_result:
        st.markdown("---")
        render_batch_results(st.session_state.last_batch_result)

elif mode == "statistics":
    st.header("📊 Статистика и аналитика")
    
    # Статистика из БД
    render_processing_statistics(processor)
    
    st.markdown("---")
    
    # Дополнительная аналитика
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔍 Анализ данных")
        
        # Статистика по брендам
        try:
            brands_query = """
            SELECT 
                ocp.oz_brand,
                COUNT(*) as total_products,
                COUNT(CASE WHEN ocp.rich_content_json IS NOT NULL AND ocp.rich_content_json != '' 
                      THEN 1 END) as with_rich_content,
                COUNT(CASE WHEN op.oz_fbo_stock > 0 THEN 1 END) as in_stock
            FROM oz_category_products ocp
            LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
            WHERE ocp.oz_brand IS NOT NULL
            GROUP BY ocp.oz_brand
            ORDER BY total_products DESC
            LIMIT 10
            """
            
            brands_df = pd.read_sql(brands_query, conn)
            
            if not brands_df.empty:
                brands_df['coverage_%'] = (brands_df['with_rich_content'] / brands_df['total_products'] * 100).round(1)
                
                st.write("**ТОП-10 брендов по количеству товаров:**")
                st.dataframe(
                    brands_df[['oz_brand', 'total_products', 'with_rich_content', 'coverage_%']],
                    use_container_width=True
                )
        except Exception as e:
            st.warning(f"Не удалось загрузить статистику по брендам: {e}")
    
    with col2:
        st.subheader("⚡ Быстрые действия")
        
        # Быстрые операции
        if st.button("🔄 Обновить статистику", use_container_width=True):
            st.rerun()
        
        if st.button("🧹 Очистить кэш", use_container_width=True):
            # Очистка кэша процессора
            if hasattr(processor, 'data_collector'):
                processor.data_collector.clear_cache()
            st.success("Кэш очищен!")
        
        # Экспорт результатов
        if st.session_state.last_batch_result:
            st.write("**Последние результаты:**")
            
            # Подготовка данных для экспорта
            export_data = []
            for result in st.session_state.last_batch_result.processed_items:
                export_data.append({
                    'oz_vendor_code': result.oz_vendor_code,
                    'status': result.status.value,
                    'recommendations_count': len(result.recommendations),
                    'processing_time_ms': round(result.processing_time * 1000, 1),
                    'has_rich_content': bool(result.rich_content_json),
                    'error_message': result.error_message or ''
                })
            
            if export_data:
                export_df = pd.DataFrame(export_data)
                csv = export_df.to_csv(index=False).encode('utf-8')
                
                st.download_button(
                    label="📥 Скачать результаты CSV",
                    data=csv,
                    file_name=f"rich_content_results_{int(time.time())}.csv",
                    mime="text/csv",
                    use_container_width=True
                )

# --- Справка в сайдбаре ---
with st.sidebar.expander("❓ Справка", expanded=False):
    st.markdown("""
    **🎯 Один товар:**
    - Обработка конкретного артикула
    - Превью Rich Content JSON
    - Ручное сохранение
    
    **📦 Пакетная обработка:**
    - Множественный выбор товаров
    - Автоматическое сохранение
    - Прогресс обработки
    
    **📊 Статистика:**
    - Покрытие Rich Content
    - Анализ по брендам
    - Экспорт результатов
    
    **⚙️ Новые возможности:**
    - Настройка минимального количества рекомендаций
    - Управление на основной странице
    """)

# --- Footer ---
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: #666;'>"
    "🚀 Rich Content Generator для Ozon • Powered by AI Recommendations"
    "</div>", 
    unsafe_allow_html=True
) 