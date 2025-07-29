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
                            # Получаем информацию о родительском товаре для передачи в генератор
                            source_product = processor.recommendation_engine.data_collector.get_full_product_info(vendor_code.strip())
                            updated_json = processor.content_generator.generate_rich_content_json(
                                result.recommendations, 
                                template_type,
                                parent_product=source_product
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
            
            # Выбор режима обработки
            processing_mode = st.radio(
                "Режим обработки:",
                ["standard", "optimized", "memory_safe"],
                format_func=lambda x: {
                    "standard": "🐌 Стандартная обработка",
                    "optimized": "⚡ Оптимизированная обработка",
                    "memory_safe": "💾 Безопасный режим (для больших объемов)"
                }[x],
                index=1,  # По умолчанию оптимизированная
                help="Безопасный режим рекомендуется для >1000 товаров - сохраняет сразу в БД без накопления в памяти"
            )
            
            # Настройки для оптимизированной обработки
            batch_size = 50
            if processing_mode == "optimized":
                batch_size = st.number_input(
                    "Размер батча:",
                    min_value=10,
                    max_value=200,
                    value=50,
                    step=10,
                    help="Количество товаров для обработки в одном батче. Больше = быстрее, но больше нагрузка на память"
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
                    if processing_mode == "memory_safe":
                        st.info(f"💾 Используется безопасный режим - прямое сохранение в БД")
                        
                        # Безопасный режим для больших объемов
                        stats = {
                            'total': len(selected_products),
                            'successful': 0,
                            'errors': 0,
                            'start_time': time.time()
                        }
                        
                        # Обрабатываем по одному товару с прямым сохранением
                        for i, vendor_code in enumerate(selected_products):
                            try:
                                progress_callback(i + 1, len(selected_products), f"Обрабатываем {vendor_code}")
                                
                                result = processor.process_single_product(vendor_code)
                                
                                if result.success and auto_save:
                                    success = processor.save_rich_content_to_database(result)
                                    if success:
                                        stats['successful'] += 1
                                    else:
                                        stats['errors'] += 1
                                elif result.success:
                                    stats['successful'] += 1
                                else:
                                    stats['errors'] += 1
                                    
                            except Exception as e:
                                stats['errors'] += 1
                                logger.error(f"Ошибка обработки {vendor_code}: {e}")
                        
                        stats['processing_time'] = time.time() - stats['start_time']
                        
                        # Создаем минимальный результат только со статистикой
                        batch_result = type('BatchResult', (), {
                            'total_items': stats['total'],
                            'processed_items': [],  # Пустой список для экономии памяти
                            'stats': {
                                'successful': stats['successful'],
                                'errors': stats['errors'],
                                'success_rate': round(stats['successful'] / stats['total'] * 100, 2) if stats['total'] > 0 else 0
                            },
                            'success': stats['successful'] > 0,
                            'is_memory_safe': True
                        })()
                        
                        st.session_state.last_batch_result = batch_result
                        
                    elif processing_mode == "optimized":
                        st.info(f"⚡ Используется оптимизированная обработка (размер батча: {batch_size})")
                        batch_result = processor.process_batch_optimized(
                            selected_products, 
                            progress_callback,
                            batch_size=batch_size
                        )
                    else:
                        st.info("🐌 Используется стандартная обработка")
                        batch_result = processor.process_batch(
                            selected_products, 
                            progress_callback
                        )
                    
                    processing_time = time.time() - start_time
                    
                    # Создаем легковесную версию для больших пакетов
                    if len(batch_result.processed_items) > 1000:
                        st.warning("⚠️ **Большой пакет обнаружен** - создаем легковесную версию результатов для предотвращения переполнения памяти.")
                        
                        # Создаем сокращенную версию без heavy данных
                        lightweight_items = []
                        for item in batch_result.processed_items:
                            lightweight_item = type('ProcessingResult', (), {
                                'oz_vendor_code': item.oz_vendor_code,
                                'status': item.status,
                                'success': item.success,
                                'processing_time': item.processing_time,
                                'error_message': item.error_message,
                                'recommendations': [],  # Очищаем тяжелые данные
                                'rich_content_json': None  # Убираем JSON для экономии памяти
                            })()
                            lightweight_items.append(lightweight_item)
                        
                        # Создаем легковесный batch_result
                        lightweight_batch_result = type('BatchResult', (), {
                            'total_items': batch_result.total_items,
                            'processed_items': lightweight_items,
                            'stats': batch_result.stats,
                            'success': batch_result.success,
                            'is_lightweight': True  # Метка что это легковесная версия
                        })()
                        
                        st.session_state.last_batch_result = lightweight_batch_result
                        
                        # Показываем информацию о том, что было сделано
                        st.info(f"💾 **Память оптимизирована:** Результаты сохранены в сокращенном виде. Для экспорта используйте 'Альтернативный экспорт из БД'.")
                        
                    else:
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
                    
                    # Добавляем кнопку для освобождения памяти
                    if len(batch_result.processed_items) > 1000:
                        st.info("💡 **Совет:** После экспорта данных рекомендуется очистить память для улучшения производительности.")
                        if st.button("🧹 Очистить память (данные останутся в БД)", 
                                   help="Очищает результаты из памяти браузера, но данные сохраняются в базе данных"):
                            # Создаем lightweight версию результатов только со статистикой
                            lightweight_result = type('BatchResult', (), {
                                'total_items': batch_result.total_items,
                                'processed_items': [],  # Очищаем тяжелые данные
                                'stats': batch_result.stats,
                                'success': batch_result.success
                            })()
                            st.session_state.last_batch_result = lightweight_result
                            st.rerun()
        
        with col2:
            st.subheader("ℹ️ Информация")
            st.info(f"**Выбрано товаров:** {len(selected_products)}")
            st.info(f"**Режим выбора:** {selection_mode}")
            
            # Прогнозируемое время обработки
            if len(selected_products) > 0:
                if processing_mode == "optimized":
                    estimated_time = len(selected_products) * 0.2  # ~0.2 секунды на товар
                    st.success(f"⚡ **Прогноз времени (оптимизированная):** ~{estimated_time:.1f} сек")
                else:
                    estimated_time = len(selected_products) * 1.0  # ~1 секунда на товар
                    st.warning(f"🐌 **Прогноз времени (стандартная):** ~{estimated_time:.1f} сек")
            
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
        
        # Защита от WebSocket ошибок при отображении больших результатов
        try:
            render_batch_results(st.session_state.last_batch_result)
        except Exception as e:
            st.error(f"⚠️ **Ошибка отображения результатов:** {str(e)}")
            st.warning("""
            🔧 **Возможные решения:**
            1. Обновите страницу (F5)
            2. Используйте 'Альтернативный экспорт из БД' ниже
            3. Очистите память кнопкой ниже
            """)
            
            if st.button("🧹 Очистить результаты и освободить память"):
                st.session_state.last_batch_result = None
                st.rerun()
        
        # Экспорт Rich Content CSV
        st.markdown("---")
        st.subheader("📥 Экспорт Rich Content")
        
        # Диагностическая информация
        total_processed = len(st.session_state.last_batch_result.processed_items)
        st.info(f"🔍 **Диагностика:** Всего обработано результатов: {total_processed}")
        
        # Проверяем, легковесная ли версия результатов
        is_lightweight = getattr(st.session_state.last_batch_result, 'is_lightweight', False)
        is_memory_safe = getattr(st.session_state.last_batch_result, 'is_memory_safe', False)
        
        if is_lightweight or is_memory_safe:
            st.warning("""
            ⚠️ **Оптимизированная версия результатов**
            
            Для экономии памяти Rich Content данные не загружены в браузер.
            Используйте **'Экспорт из БД'** для получения полных данных.
            """)
            successful_results = []
            empty_rich_content_count = 0
        else:
            # Подготовка данных для экспорта Rich Content с улучшенной логикой
            successful_results = []
            empty_rich_content_count = 0
            
            for result in st.session_state.last_batch_result.processed_items:
                if result.success:
                    # Проверяем, что rich_content_json не None и не пустая строка
                    if result.rich_content_json and result.rich_content_json.strip():
                        successful_results.append(result)
                    else:
                        empty_rich_content_count += 1
        
        # Показываем детальную диагностику
        success_count = sum(1 for r in st.session_state.last_batch_result.processed_items if r.success)
        st.info(f"✅ **Успешно обработано:** {success_count} товаров")
        st.info(f"📄 **С Rich Content JSON:** {len(successful_results)} товаров")
        if empty_rich_content_count > 0:
            st.warning(f"⚠️ **Пустой Rich Content:** {empty_rich_content_count} товаров")
        
        # Всегда показываем опции экспорта
        st.markdown("### 🚀 Варианты экспорта Rich Content")
        
        # Создаем колонки для разных вариантов экспорта
        export_col1, export_col2, export_col3 = st.columns(3)
        
        with export_col1:
            st.markdown("#### 📋 Быстрый экспорт")
            st.caption("Данные из памяти (если доступны)")
            
            if successful_results:
                # Проверка размера данных и предупреждения
                estimated_size_mb = len(successful_results) * 10 / 1024  # Примерно 10 KB на товар
                if estimated_size_mb > 50:
                    st.warning(f"⚠️ **Большой объем:** ~{estimated_size_mb:.1f} МБ")
                
                st.success(f"📥 **Доступно:** {len(successful_results)} товаров")
                
                if st.button("📥 Скачать (из памяти)", use_container_width=True):
                    try:
                        with st.spinner("📝 Подготовка данных для экспорта..."):
                            # Оптимизированная подготовка данных с пакетными запросами
                            vendor_codes = [result.oz_vendor_code for result in successful_results]
                            
                            # Пакетный запрос для получения всех oz_sku из oz_products
                            if vendor_codes:
                                placeholders = ','.join(['?' for _ in vendor_codes])
                                sku_query = f"""
                                SELECT oz_vendor_code, oz_sku 
                                FROM oz_products 
                                WHERE oz_vendor_code IN ({placeholders})
                                """
                                sku_results = conn.execute(sku_query, vendor_codes).fetchall()
                                sku_map = {row[0]: row[1] if row[1] else "" for row in sku_results}
                            else:
                                sku_map = {}
                            
                            # Подготовка данных для экспорта
                            rich_content_data = []
                            progress_bar = st.progress(0)
                            
                            for i, result in enumerate(successful_results):
                                oz_sku = sku_map.get(result.oz_vendor_code, "")
                                
                                rich_content_data.append({
                                    'oz_vendor_code': result.oz_vendor_code,
                                    'oz_sku': oz_sku,
                                    'rich_content': result.rich_content_json
                                })
                                
                                # Обновляем прогресс каждые 100 товаров
                                if i % 100 == 0:
                                    progress_bar.progress((i + 1) / len(successful_results))
                            
                            progress_bar.progress(1.0)
                            
                            if rich_content_data:
                                # Создаем DataFrame и конвертируем в CSV
                                rich_content_df = pd.DataFrame(rich_content_data)
                                csv_content = rich_content_df.to_csv(index=False).encode('utf-8')
                                
                                # Показываем информацию о размере файла
                                file_size_mb = len(csv_content) / (1024 * 1024)
                                st.info(f"📊 **Размер файла:** {file_size_mb:.2f} МБ")
                                
                                # Кнопка скачивания
                                st.download_button(
                                    label="💾 Скачать готовый CSV файл",
                                    data=csv_content,
                                    file_name=f"rich_content_export_{int(time.time())}.csv",
                                    mime="text/csv",
                                    use_container_width=True
                                )
                                
                                # Показываем превью данных
                                with st.expander("👁️ Превью экспортируемых данных", expanded=False):
                                    st.dataframe(
                                        rich_content_df[['oz_vendor_code', 'oz_sku']].head(20),
                                        use_container_width=True
                                    )
                                    st.caption(f"Rich Content JSON включен в экспорт (показаны первые 20 записей)")
                                    
                                # Очищаем временные данные
                                del rich_content_data, rich_content_df, csv_content
                                
                            else:
                                st.error("❌ Не удалось подготовить данные для экспорта")
                                
                    except Exception as e:
                        st.error(f"❌ Ошибка при подготовке экспорта: {e}")
                        st.info("💡 Попробуйте обновить страницу или использовать экспорт из БД")
            else:
                st.warning("⚠️ Нет данных в памяти")
                st.info("Используйте экспорт из БД →")
        
        with export_col2:
            st.markdown("#### 🗄️ Экспорт из БД")
            st.caption("Все данные из базы данных")
            
            if st.button("🔄 Экспорт из БД", use_container_width=True):
                try:
                    with st.spinner("📝 Экспорт напрямую из базы данных..."):
                        # Получаем данные напрямую из БД с правильным JOIN
                        db_query = """
                        SELECT 
                            ocp.oz_vendor_code,
                            COALESCE(op.oz_sku, '') as oz_sku,
                            ocp.rich_content_json
                        FROM oz_category_products ocp
                        LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
                        WHERE ocp.rich_content_json IS NOT NULL 
                        AND ocp.rich_content_json != ''
                        AND LENGTH(ocp.rich_content_json) > 10
                        ORDER BY ocp.oz_vendor_code
                        """
                        
                        db_results = conn.execute(db_query).fetchall()
                        
                        if db_results:
                            # Создаем DataFrame напрямую из результатов
                            db_df = pd.DataFrame(db_results, columns=['oz_vendor_code', 'oz_sku', 'rich_content'])
                            db_csv_content = db_df.to_csv(index=False).encode('utf-8')
                            
                            # Показываем информацию о размере файла
                            file_size_mb = len(db_csv_content) / (1024 * 1024)
                            st.success(f"✅ Найдено {len(db_results)} товаров с Rich Content")
                            st.info(f"📊 **Размер файла:** {file_size_mb:.2f} МБ")
                            
                            st.download_button(
                                label="💾 Скачать из БД",
                                data=db_csv_content,
                                file_name=f"rich_content_from_db_{int(time.time())}.csv",
                                mime="text/csv",
                                use_container_width=True
                            )
                            
                            # Показываем превью данных
                            with st.expander("👁️ Превью данных из БД", expanded=False):
                                st.dataframe(
                                    db_df[['oz_vendor_code', 'oz_sku']].head(20),
                                    use_container_width=True
                                )
                                st.caption(f"Rich Content JSON включен в экспорт (показаны первые 20 записей)")
                            
                        else:
                            st.warning("⚠️ В базе данных не найдено товаров с Rich Content")
                            
                except Exception as e:
                    st.error(f"❌ Ошибка экспорта из БД: {e}")
        
        with export_col3:
            st.markdown("#### 🎯 Потоковый экспорт")
            st.caption("Для очень больших объемов")
            
            if st.button("🌊 Потоковый экспорт", use_container_width=True):
                try:
                    with st.spinner("📝 Потоковый экспорт больших данных..."):
                        # Сначала получаем количество записей
                        count_query = """
                        SELECT COUNT(*) 
                        FROM oz_category_products ocp
                        LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
                        WHERE ocp.rich_content_json IS NOT NULL 
                        AND ocp.rich_content_json != ''
                        AND LENGTH(ocp.rich_content_json) > 10
                        """
                        
                        total_count = conn.execute(count_query).fetchone()[0]
                        
                        if total_count > 0:
                            st.info(f"📊 Найдено {total_count} записей для экспорта")
                            
                            # Генерируем CSV потоком по частям
                            def generate_csv_stream():
                                # Заголовки CSV
                                yield "oz_vendor_code,oz_sku,rich_content\n"
                                
                                batch_size = 1000
                                offset = 0
                                
                                while offset < total_count:
                                    batch_query = """
                                    SELECT 
                                        ocp.oz_vendor_code,
                                        COALESCE(op.oz_sku, '') as oz_sku,
                                        ocp.rich_content_json
                                    FROM oz_category_products ocp
                                    LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
                                    WHERE ocp.rich_content_json IS NOT NULL 
                                    AND ocp.rich_content_json != ''
                                    AND LENGTH(ocp.rich_content_json) > 10
                                    ORDER BY ocp.oz_vendor_code
                                    LIMIT ? OFFSET ?
                                    """
                                    
                                    batch_results = conn.execute(batch_query, [batch_size, offset]).fetchall()
                                    
                                    if not batch_results:
                                        break
                                    
                                    # Конвертируем batch в CSV строки
                                    for row in batch_results:
                                        # Экранируем кавычки в JSON
                                        escaped_json = str(row[2]).replace('"', '""')
                                        yield f'"{row[0]}","{row[1]}","{escaped_json}"\n'
                                    
                                    offset += batch_size
                            
                            # Создаем итератор для потокового экспорта
                            csv_stream = generate_csv_stream()
                            
                            # Объединяем все части в один файл
                            csv_content = ''.join(csv_stream).encode('utf-8')
                            
                            # Показываем информацию о размере файла
                            file_size_mb = len(csv_content) / (1024 * 1024)
                            st.success(f"✅ Потоковый экспорт завершен")
                            st.info(f"📊 **Размер файла:** {file_size_mb:.2f} МБ")
                            
                            st.download_button(
                                label="💾 Скачать потоковый CSV",
                                data=csv_content,
                                file_name=f"rich_content_streaming_{int(time.time())}.csv",
                                mime="text/csv",
                                use_container_width=True
                            )
                        else:
                            st.warning("⚠️ Нет данных для потокового экспорта")
                            
                except Exception as e:
                    st.error(f"❌ Ошибка потокового экспорта: {e}")
                    st.info("💡 Попробуйте обычный экспорт из БД")

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
    - ⚡ Оптимизированная обработка (3-5x быстрее)
    
    **📊 Статистика:**
    - Покрытие Rich Content
    - Анализ по брендам
    - Экспорт результатов
    
    **⚙️ Оптимизации:**
    - Группировка товаров по типу/полу/бренду
    - Пакетное обогащение данными
    - Настройка размера батча
    - Прогноз времени обработки
    
    **🆘 Для больших объемов:**
    - Используйте страницу "🆘 Экстренный Экспорт"
    - CLI утилита: utils/export_rich_content.py
    - Прямой доступ к БД без ограничений памяти
    """)

# --- Footer ---
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: #666;'>"
    "🚀 Rich Content Generator для Ozon • Powered by AI Recommendations"
    "</div>", 
    unsafe_allow_html=True
) 