"""
Streamlit page for generating custom analytic reports.

This page allows users to:
- Select or upload a custom analytic report Excel file
- Process WB SKUs to find related Ozon data
- Generate comprehensive reports with size distribution, stock data, and order statistics
- Update the Excel file with calculated data while preserving structure
"""
import streamlit as st
import os
from utils.db_connection import connect_db
from utils import config_utils
from utils.analytic_report_helpers import process_analytic_report, load_analytic_report_file
import pandas as pd

st.set_page_config(page_title="Analytic Report Generation - Marketplace Analyzer", layout="wide")
st.title("📈 Custom Analytic Report Generation")
st.markdown("---")

# --- Introduction ---
st.markdown("""
### О функции
Эта страница позволяет генерировать пользовательские аналитические отчеты на основе данных WB SKU.
Система автоматически находит связанные Ozon SKU и заполняет данные по размерам, остаткам, статистике заказов и справочной информации Punta.

### Требования к файлу
- Файл должен быть в формате .xlsx
- Обязательно наличие листа "analytic_report"
- 7-я строка должна содержать заголовки колонок
- 8-я строка игнорируется (может содержать описания)
- Данные должны начинаться с 9-й строки
- Обязательная колонка: **WB_SKU**

### Поддерживаемые типы колонок
- **OZ_SIZE_XX**: Размеры обуви (22-44) с Ozon SKU
- **OZ_SIZES**: Диапазон доступных размеров
- **OZ_STOCK**: Суммарный остаток Ozon
- **ORDERS_TODAY-XX**: Статистика заказов по дням
- **PUNTA_XXX**: Справочные данные из таблицы Punta (NEW!)
  - Например: PUNTA_season, PUNTA_gender, PUNTA_material
- **PHOTO_FROM_WB**: Изображения товаров с WB (опционально)
  - По умолчанию отключено для быстрой обработки  
  - При включении загружает и вставляет изображения товаров из Wildberries
  - Изображения вставляются непосредственно в ячейки Excel
""")

# --- Database Connection ---
conn = connect_db()
if not conn:
    st.error("❌ База данных не подключена. Пожалуйста, настройте подключение в настройках.")
    if st.button("Go to Settings"):
        st.switch_page("pages/3_Settings.py")
    st.stop()

# --- File Selection ---
st.subheader("📁 Выбор файла отчета")

col1, col2 = st.columns([2, 1])

with col1:
    # Get default path from settings
    default_path = config_utils.get_report_path("analytic_report_xlsx")
    
    # File selection method
    file_source = st.radio(
        "Как выбрать файл:",
        ["📂 Использовать путь из настроек", "⬆️ Загрузить файл"],
        index=0 if default_path else 1
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
        st.warning("⚠️ Путь к файлу не настроен. Перейдите в Settings для настройки или выберите загрузку файла.")

else:  # Upload file
    uploaded_file = st.file_uploader(
        "Выберите Excel файл с аналитическим отчетом:",
        type=['xlsx'],
        help="Файл должен содержать лист 'analytic_report' с правильной структурой"
    )
    
    if uploaded_file is not None:
        # Save uploaded file temporarily
        temp_path = f"temp_analytic_report_{uploaded_file.name}"
        with open(temp_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        selected_file_path = temp_path
        st.success(f"✅ Файл загружен: `{uploaded_file.name}`")

# --- File Validation and Preview ---
if selected_file_path:
    st.markdown("---")
    st.subheader("🔍 Валидация и предпросмотр файла")
    
    # Validate file structure
    df, wb, error_msg = load_analytic_report_file(selected_file_path)
    
    if df is not None:
        st.success("✅ Файл успешно загружен и валидирован")
        
        # Display file statistics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📊 Строк с данными", len(df))
        with col2:
            wb_sku_count = len(df['WB_SKU'].dropna().unique())
            st.metric("🏷️ Уникальных WB SKU", wb_sku_count)
        with col3:
            st.metric("📋 Колонок в файле", len(df.columns))
        with col4:
            if wb_sku_count > 1000:
                st.metric("⚠️ Размер файла", "Большой", delta="Обработка может занять время")
            else:
                st.metric("✅ Размер файла", "Оптимальный")
        
        # Preview data
        with st.expander("👀 Предпросмотр данных (первые 5 строк)", expanded=False):
            # Show only relevant columns for preview
            preview_cols = ['WB_SKU'] + [col for col in df.columns if col.startswith('OZ_SIZE_') or col in ['OZ_SIZES', 'OZ_STOCK'] or col.startswith('ORDERS_TODAY-') or col.startswith('PUNTA_')][:15]
            preview_cols = [col for col in preview_cols if col in df.columns]
            st.dataframe(df[preview_cols].head(), use_container_width=True, hide_index=True)
        
        # Show expected columns that will be filled
        with st.expander("📝 Колонки, которые будут заполнены", expanded=False):
            expected_cols = []
            
            # Size columns
            size_cols = [f"OZ_SIZE_{i}" for i in range(22, 45)]
            found_size_cols = [col for col in size_cols if col in df.columns]
            if found_size_cols:
                expected_cols.append(f"**Размеры ({len(found_size_cols)} колонок)**: {', '.join(found_size_cols[:5])}{'...' if len(found_size_cols) > 5 else ''}")
            
            # Summary columns
            summary_cols = ['OZ_SIZES', 'OZ_STOCK']
            found_summary_cols = [col for col in summary_cols if col in df.columns]
            if found_summary_cols:
                expected_cols.append(f"**Сводная информация**: {', '.join(found_summary_cols)}")
            
            # Order columns
            order_cols = [col for col in df.columns if col.startswith('ORDERS_TODAY-')]
            if order_cols:
                expected_cols.append(f"**Статистика заказов ({len(order_cols)} колонок)**: {', '.join(order_cols[:3])}{'...' if len(order_cols) > 3 else ''}")
            
            # Punta columns
            punta_cols = [col for col in df.columns if col.startswith('PUNTA_')]
            if punta_cols:
                expected_cols.append(f"**Справочные данные Punta ({len(punta_cols)} колонок)**: {', '.join(punta_cols[:3])}{'...' if len(punta_cols) > 3 else ''}")
            
            # Photo column
            if 'PHOTO_FROM_WB' in df.columns:
                expected_cols.append("**🖼️ Изображения товаров**: PHOTO_FROM_WB - опциональная загрузка изображений с WB (по умолчанию отключено)")
            
            if expected_cols:
                for col_info in expected_cols:
                    st.markdown(f"- {col_info}")
            else:
                st.warning("Не найдено ожидаемых колонок для заполнения. Проверьте структуру файла.")
        
        # --- Processing Section ---
        st.markdown("---")
        st.subheader("🚀 Обработка отчета")
        
        if wb_sku_count == 0:
            st.error("❌ В файле нет валидных WB SKU для обработки")
        else:
            st.info(f"📋 Будет обработано **{wb_sku_count}** уникальных WB SKU")
            
            # Warning for large files
            if wb_sku_count > 500:
                st.warning("⚠️ **Большой файл**: Обработка может занять несколько минут. Не закрывайте страницу во время обработки.")
            
            # Options section
            st.markdown("#### ⚙️ Опции обработки")
            
            col1, col2 = st.columns([1, 2])
            
            with col1:
                include_images = st.checkbox(
                    "🖼️ Загружать изображения товаров",
                    value=False,
                    help="Включить автоматическую загрузку и вставку изображений WB в колонку PHOTO_FROM_WB"
                )
            
            with col2:
                if 'PHOTO_FROM_WB' in df.columns:
                    if include_images:
                        st.success("✅ Изображения будут загружены и вставлены в Excel")
                    else:
                        st.info("📷 Колонка PHOTO_FROM_WB будет очищена (изображения не загружаются)")
                else:
                    st.info("ℹ️ Колонка PHOTO_FROM_WB не найдена в файле")
            
            if include_images and wb_sku_count > 100:
                st.warning("⚠️ **Загрузка изображений**: Может значительно увеличить время обработки. Убедитесь в стабильном интернет-соединении.")
            
            # Processing button
            process_button = st.button(
                "🔄 Обработать отчет",
                type="primary",
                help="Запустить обработку всех WB SKU и обновление файла",
                use_container_width=True
            )
            
            if process_button:
                # Start processing
                with st.spinner("🔄 Обработка отчета... Это может занять некоторое время..."):
                    
                    # Create progress tracking
                    progress_container = st.container()
                    with progress_container:
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        status_text.text("Инициализация обработки...")
                        progress_bar.progress(10)
                        
                        # Process the report
                        success, error_msg, stats = process_analytic_report(conn, selected_file_path, include_images)
                        
                        if success:
                            progress_bar.progress(100)
                            status_text.text("✅ Обработка завершена успешно!")
                            
                            st.success("🎉 **Отчет успешно обработан и обновлен!**")
                            
                            # Display statistics
                            st.markdown("### 📊 Статистика обработки")
                            col1, col2, col3, col4, col5 = st.columns(5)
                            
                            with col1:
                                st.metric(
                                    "📋 Обработано WB SKU",
                                    stats.get('processed_wb_skus', 0)
                                )
                            with col2:
                                st.metric(
                                    "🔗 Найдены совпадения Ozon",
                                    stats.get('wb_skus_with_ozon_matches', 0),
                                    delta=f"{stats.get('wb_skus_with_ozon_matches', 0) / max(stats.get('processed_wb_skus', 1), 1) * 100:.1f}%"
                                )
                            with col3:
                                st.metric(
                                    "🏷️ Найдено Ozon SKU",
                                    stats.get('total_ozon_skus_found', 0)
                                )
                            with col4:
                                st.metric(
                                    "📦 Общий остаток",
                                    f"{stats.get('total_stock', 0):,}".replace(',', ' ')
                                )
                            with col5:
                                st.metric(
                                    "📚 Найдены данные Punta",
                                    stats.get('wb_skus_with_punta_data', 0),
                                    delta=f"{stats.get('wb_skus_with_punta_data', 0) / max(stats.get('processed_wb_skus', 1), 1) * 100:.1f}%"
                                )
                            
                            # Success actions
                            st.markdown("### 🎯 Что дальше?")
                            
                            # Different handling for uploaded vs configured files
                            if selected_file_path.startswith('temp_'):
                                # For uploaded files - offer download
                                st.info("📁 **Загруженный файл**: Файл был обработан. Скачайте обновленную версию ниже.")
                                
                                col1, col2 = st.columns(2)
                                with col1:
                                    # Provide download button
                                    with open(selected_file_path, "rb") as file:
                                        btn = st.download_button(
                                            label="💾 Скачать обработанный файл",
                                            data=file.read(),
                                            file_name=f"processed_{uploaded_file.name}",
                                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                            use_container_width=True
                                        )
                                
                                with col2:
                                    if st.button("🔄 Обработать другой файл", use_container_width=True):
                                        # Clean up temp file
                                        try:
                                            os.remove(selected_file_path)
                                        except:
                                            pass
                                        st.rerun()
                                
                                st.warning("⚠️ **Важно**: Резервная копия не была создана для загруженного файла. Сохраните обработанный файл в нужном месте.")
                                
                            else:
                                # For configured files - normal backup created
                                col1, col2 = st.columns(2)
                                
                                with col1:
                                    st.info("📁 **Резервная копия**: Оригинальный файл сохранен с timestamp")
                                    st.info("💾 **Обновленный файл**: Данные записаны в исходный файл")
                                
                                with col2:
                                    if st.button("🔄 Обработать другой файл", use_container_width=True):
                                        st.rerun()
                            
                            st.balloons()
                            
                        else:
                            progress_bar.progress(0)
                            status_text.text("❌ Ошибка при обработке")
                            st.error(f"❌ **Ошибка при обработке отчета:**\n\n{error_msg}")
                            
                            # Error help
                            with st.expander("🔧 Рекомендации по устранению ошибок"):
                                st.markdown("""
                                **Возможные причины ошибок:**
                                1. **Файл заблокирован**: Закройте Excel файл, если он открыт
                                2. **Недостаточно прав**: Проверьте права на запись в папку с файлом
                                3. **Нет связанных данных**: Убедитесь, что WB SKU есть в базе данных
                                4. **Повреждена структура**: Проверьте соответствие структуры файла требованиям
                                5. **Проблемы с базой данных**: Убедитесь, что все необходимые таблицы загружены
                                """)
    else:
        st.error(f"❌ **Ошибка валидации файла:**\n\n{error_msg}")
        
        # Help section for file structure
        with st.expander("📖 Помощь: Требования к структуре файла"):
            st.markdown("""
            ### Правильная структура файла:
            
            1. **Лист**: Должен называться `analytic_report`
            2. **Строка 7**: Заголовки колонок (например: `N`, `WB_SKU`, `OZ_SIZE_27`, `OZ_SIZE_28`, ..., `OZ_SIZES`, `OZ_STOCK`, `ORDERS_TODAY-30`, ...)
            3. **Строка 8**: Описания колонок (игнорируется при обработке)
            4. **Строки 9+**: Данные для обработки
            
            ### Обязательные колонки:
            - **WB_SKU**: Артикул WB (обязательно)
            
            ### Опциональные колонки (будут заполнены):
            - **OZ_SIZE_22** до **OZ_SIZE_44**: Ozon SKU по размерам
            - **OZ_SIZES**: Диапазон размеров (например, "27-38")
            - **OZ_STOCK**: Суммарный остаток Ozon
            - **ORDERS_TODAY-30** до **ORDERS_TODAY-1**: Заказы по дням
            - **PUNTA_XXX**: Справочные данные из таблицы Punta
            - **PHOTO_FROM_WB**: Изображения товаров с WB (опционально)
            
            ### Пример правильного заголовка (строка 7):
            ```
            N | WB_SKU | PHOTO_FROM_WB | OZ_SIZE_27 | OZ_SIZE_28 | ... | OZ_SIZES | OZ_STOCK | ORDERS_TODAY-30 | PUNTA_season | ...
            ```
            """)

else:
    # No file selected
    st.info("👆 Выберите файл для начала работы")

# --- Help Section ---
st.markdown("---")
with st.expander("❓ Справка и примеры"):
    st.markdown("""
    ### Как работает система:
    
    1. **Загрузка файла**: Система читает Excel файл и валидирует его структуру
    2. **Поиск связей**: Для каждого WB SKU находятся связанные Ozon SKU через общие штрихкоды
    3. **Распределение по размерам**: Ozon SKU группируются по размерам обуви
    4. **Агрегация данных**: Рассчитываются остатки и статистика заказов
    5. **Загрузка изображений**: Опционально - для колонки PHOTO_FROM_WB загружаются изображения товаров с WB
    6. **Обновление файла**: Данные записываются в исходный файл с созданием резервной копии
    
    ### Что заполняется:
    - **OZ_SIZE_XX**: Ozon SKU для каждого размера обуви
    - **OZ_SIZES**: Диапазон доступных размеров
    - **OZ_STOCK**: Общий остаток по FBO схеме
    - **ORDERS_TODAY-XX**: Количество заказов за последние 30 дней
    - **PUNTA_XXX**: Справочные данные из таблицы Punta по общему WB SKU
    - **PHOTO_FROM_WB**: Изображения товаров привязанные к ячейкам (без текста)
    
    ### Справочные данные Punta:
    - **PUNTA_gender**: Пол (Мальчики/Девочки)
    - **PUNTA_season**: Сезон (Лето/Деми/Зима)
    - **PUNTA_model_name**: Название модели
    - **PUNTA_material**: Описание материала
    - **PUNTA_new_last, PUNTA_mega_last, PUNTA_best_last**: Коды last
    
    ### Функция изображений PHOTO_FROM_WB (опциональная):
    - **Включение по выбору**: Функция отключена по умолчанию для быстрой обработки
    - **Автоматическая загрузка**: При включении система генерирует URL изображения на основе WB_SKU
    - **Алгоритм WB**: Используется тот же алгоритм, что и в Wildberries для определения сервера изображений
    - **Вставка в Excel**: Изображения вставляются непосредственно в ячейки с автоматическим изменением размера строк
    - **Привязка к ячейкам**: Изображения привязаны к ячейкам и двигаются вместе с изменением их размеров
    - **Чистый интерфейс**: Ячейки содержат только изображения без дополнительного текста
    - **Конвертация формата**: WebP изображения автоматически конвертируются в PNG для совместимости с Excel
    - **Статистика загрузки**: Система показывает количество успешно загруженных изображений
    - **Быстрая обработка**: При отключении колонка очищается без загрузки изображений
    
    ### Полезные советы:
    - Настройте путь к файлу в Settings для быстрого доступа
    - Убедитесь, что файл Excel не открыт в других программах
    - Большие файлы (>500 WB SKU) могут обрабатываться несколько минут
    - Загрузка изображений отключена по умолчанию для быстрой обработки
    - При включении изображений обработка может занимать дополнительное время
    - Резервная копия создается автоматически перед каждым обновлением
    - Для лучшей производительности загрузки изображений убедитесь в стабильном интернет-соединении
    
    ### Пример правильного заголовка (строка 7):
    ```
    N | WB_SKU | PHOTO_FROM_WB | OZ_SIZE_27 | OZ_SIZE_28 | ... | OZ_SIZES | OZ_STOCK | ORDERS_TODAY-30 | PUNTA_season | ...
    ```
    
    ### Технические особенности изображений:
    - **Формат источника**: Изображения загружаются в формате WebP (стандарт WB)
    - **Формат в Excel**: Автоматическая конвертация в PNG для совместимости с Excel
    - **Размер**: Автоматическое масштабирование до 64x64 пикселей для оптимального отображения
    - **Привязка**: Изображения привязаны к ячейкам через свойство anchor
    - **Поведение**: Изображения перемещаются и изменяются вместе с ячейками
    - **Безопасность**: Используются правильные HTTP заголовки для имитации браузера
    - **Обработка ошибок**: Система корректно обрабатывает недоступные изображения и сетевые ошибки
    - **Временные файлы**: Автоматическая очистка временных PNG файлов после обработки
    """)

# Cleanup temporary files on script end
if selected_file_path and selected_file_path.startswith('temp_') and os.path.exists(selected_file_path):
    try:
        # Register cleanup function (this won't work in Streamlit, but it's good practice)
        import atexit
        atexit.register(lambda: os.remove(selected_file_path) if os.path.exists(selected_file_path) else None)
    except:
        pass

if conn:
    conn.close() 