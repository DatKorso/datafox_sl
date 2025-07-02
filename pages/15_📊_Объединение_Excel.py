import streamlit as st
import pandas as pd
import os
from io import BytesIO
import traceback
from openpyxl import load_workbook, Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import tempfile
from utils.config_utils import load_config

st.set_page_config(
    page_title="Объединение Excel",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Объединение Excel файлов")
st.markdown("Объединение нескольких Excel файлов в один с сохранением структуры")

# Инициализация состояния
if 'template_file' not in st.session_state:
    st.session_state.template_file = None
if 'additional_files' not in st.session_state:
    st.session_state.additional_files = []
if 'sheet_config' not in st.session_state:
    st.session_state.sheet_config = {}
if 'available_sheets' not in st.session_state:
    st.session_state.available_sheets = []
if 'is_merging' not in st.session_state:
    st.session_state.is_merging = False

def get_excel_sheets(file_bytes):
    """Получить список листов из Excel файла"""
    try:
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            tmp_file.write(file_bytes)
            tmp_file.flush()
            
            wb = load_workbook(tmp_file.name, read_only=True)
            sheets = wb.sheetnames
            wb.close()
            os.unlink(tmp_file.name)
            return sheets
    except Exception as e:
        st.error(f"Ошибка при чтении файла: {str(e)}")
        return []

def read_excel_sheet(file_bytes, sheet_name, start_row=0):
    """Прочитать лист Excel начиная с указанной строки"""
    try:
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            tmp_file.write(file_bytes)
            tmp_file.flush()
            
            df = pd.read_excel(tmp_file.name, sheet_name=sheet_name, header=None)
            os.unlink(tmp_file.name)
            
            if start_row > 0:
                df = df.iloc[start_row:]
            
            return df
    except Exception as e:
        st.error(f"Ошибка при чтении листа {sheet_name}: {str(e)}")
        return pd.DataFrame()

def merge_excel_files(template_bytes, additional_files_bytes, sheet_config, progress_callback=None):
    """Объединить Excel файлы согласно конфигурации с прогресс-баром"""
    try:
        # Подсчитываем общее количество операций для прогресса
        merge_sheets = [name for name, config in sheet_config.items() if config['merge']]
        total_operations = len(merge_sheets) * (len(additional_files_bytes) + 2)  # +2 для загрузки шаблона и сохранения
        current_operation = 0
        
        def update_progress(message):
            nonlocal current_operation
            current_operation += 1
            if progress_callback:
                progress_callback(current_operation / total_operations, message)
        
        update_progress("📂 Загрузка файла-шаблона...")
        
        # Создаем новую книгу на основе шаблона
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_template:
            tmp_template.write(template_bytes)
            tmp_template.flush()
            
            template_wb = load_workbook(tmp_template.name)
            
            # Для каждого листа в конфигурации
            for sheet_idx, (sheet_name, config) in enumerate(sheet_config.items()):
                if config['merge'] and sheet_name in template_wb.sheetnames:
                    start_row = config['start_row']
                    
                    update_progress(f"📋 Обработка листа '{sheet_name}'...")
                    
                    # Читаем исходный лист шаблона
                    template_df = pd.read_excel(tmp_template.name, sheet_name=sheet_name, header=None)
                    
                    # Собираем данные из дополнительных файлов
                    additional_data = []
                    for file_idx, add_file_bytes in enumerate(additional_files_bytes):
                        update_progress(f"📄 Обработка файла {file_idx + 1}/{len(additional_files_bytes)} для листа '{sheet_name}'...")
                        
                        add_df = read_excel_sheet(add_file_bytes, sheet_name, start_row)
                        if not add_df.empty:
                            additional_data.append(add_df)
                    
                    # Объединяем данные
                    if additional_data:
                        update_progress(f"🔗 Объединение данных для листа '{sheet_name}'...")
                        combined_df = pd.concat([template_df] + additional_data, ignore_index=True)
                        
                        # Записываем обратно в лист
                        ws = template_wb[sheet_name]
                        ws.delete_rows(1, ws.max_row)
                        
                        # Показываем прогресс записи для больших листов
                        total_rows = len(combined_df)
                        for r_idx, row in enumerate(dataframe_to_rows(combined_df, index=False, header=False)):
                            for c_idx, value in enumerate(row):
                                ws.cell(row=r_idx + 1, column=c_idx + 1, value=value)
                            
                            # Обновляем прогресс каждые 100 строк для больших файлов
                            if total_rows > 1000 and r_idx % 100 == 0:
                                if progress_callback:
                                    write_progress = (r_idx / total_rows) * 0.1  # 10% от общего прогресса на запись
                                    progress_callback(
                                        (current_operation + write_progress) / total_operations,
                                        f"✏️ Запись данных: {r_idx + 1}/{total_rows} строк в лист '{sheet_name}'"
                                    )
            
            update_progress("💾 Сохранение результирующего файла...")
            
            # Сохраняем результат
            output = BytesIO()
            template_wb.save(output)
            output.seek(0)
            
            template_wb.close()
            os.unlink(tmp_template.name)
            
            update_progress("✅ Объединение завершено!")
            
            return output.getvalue()
            
    except Exception as e:
        st.error(f"Ошибка при объединении файлов: {str(e)}")
        st.error(traceback.format_exc())
        return None

# Основной интерфейс
col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("📄 Шаблон файла")
    st.markdown("Загрузите основной файл, который будет служить шаблоном")
    
    template_upload = st.file_uploader(
        "Выберите файл-шаблон",
        type=['xlsx'],
        key="template_uploader"
    )
    
    if template_upload is not None:
        st.session_state.template_file = template_upload.getvalue()
        st.session_state.available_sheets = get_excel_sheets(st.session_state.template_file)
        # Сбрасываем процесс объединения при загрузке нового шаблона
        st.session_state.is_merging = False
        st.success(f"✅ Шаблон загружен: {template_upload.name}")
        
        if st.session_state.available_sheets:
            st.info(f"🗂️ Найдено листов: {', '.join(st.session_state.available_sheets)}")

with col2:
    st.subheader("📁 Дополнительные файлы")
    st.markdown("Загрузите файлы, данные которых нужно добавить к шаблону")
    
    additional_uploads = st.file_uploader(
        "Выберите дополнительные файлы",
        type=['xlsx'],
        accept_multiple_files=True,
        key="additional_uploader"
    )
    
    if additional_uploads:
        st.session_state.additional_files = [f.getvalue() for f in additional_uploads]
        # Сбрасываем процесс объединения при загрузке новых дополнительных файлов
        st.session_state.is_merging = False
        st.success(f"✅ Загружено файлов: {len(additional_uploads)}")
        for i, f in enumerate(additional_uploads):
            st.info(f"📄 {i+1}. {f.name}")

# Конфигурация объединения
if st.session_state.template_file and st.session_state.available_sheets:
    st.subheader("⚙️ Настройка объединения")
    st.markdown("Выберите какие листы объединять и с какой строки брать данные из дополнительных файлов")
    
    config_cols = st.columns(3)
    
    with config_cols[0]:
        st.markdown("**Лист**")
    with config_cols[1]:
        st.markdown("**Объединять**")
    with config_cols[2]:
        st.markdown("**Начальная строка**")
    
    # Создаем конфигурацию для каждого листа
    for sheet in st.session_state.available_sheets:
        config_cols = st.columns(3)
        
        with config_cols[0]:
            st.write(sheet)
        
        with config_cols[1]:
            merge_checkbox = st.checkbox(
                "Объединить",
                key=f"merge_{sheet}",
                value=sheet in ["Шаблон", "Озон.Видео", "Озон.Видеообложка"]
            )
        
        with config_cols[2]:
            start_row = st.number_input(
                "Строка",
                min_value=0,
                max_value=100,
                value=5 if merge_checkbox else 0,
                key=f"start_row_{sheet}",
                disabled=not merge_checkbox
            )
        
        st.session_state.sheet_config[sheet] = {
            'merge': merge_checkbox,
            'start_row': start_row
        }

# Кнопка объединения
if st.session_state.template_file and st.session_state.additional_files and st.session_state.sheet_config:
    st.subheader("🚀 Выполнить объединение")
    
    # Показываем что будет объединено
    merge_sheets = [sheet for sheet, config in st.session_state.sheet_config.items() if config['merge']]
    if merge_sheets:
        st.info(f"📋 Будут объединены листы: {', '.join(merge_sheets)}")
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            # Определяем текст и состояние кнопки
            button_text = "⏳ Обработка..." if st.session_state.is_merging else "🔗 Объединить файлы"
            button_disabled = st.session_state.is_merging
            
            if st.button(button_text, type="primary", use_container_width=True, disabled=button_disabled):
                # Устанавливаем флаг начала обработки
                st.session_state.is_merging = True
                st.rerun()  # Перерисовываем интерфейс с заблокированной кнопкой
        
        # Обработка объединения (только если процесс активен)
        if st.session_state.is_merging:
            # Создаем контейнеры для прогресс-бара
            progress_container = st.container()
            
            with progress_container:
                progress_bar = st.progress(0)
                progress_text = st.empty()
                
                # Callback функция для обновления прогресса
                def update_progress(value, message):
                    progress_bar.progress(min(value, 1.0))
                    progress_text.text(message)
                
                try:
                    # Показываем информацию о процессе
                    merge_sheets_count = len([s for s, c in st.session_state.sheet_config.items() if c['merge']])
                    additional_files_count = len(st.session_state.additional_files)
                    
                    st.info(f"🔄 Начинается объединение: {merge_sheets_count} листов × {additional_files_count + 1} файлов")
                    
                    # Выполняем объединение с прогресс-баром
                    result_bytes = merge_excel_files(
                        st.session_state.template_file,
                        st.session_state.additional_files,
                        st.session_state.sheet_config,
                        progress_callback=update_progress
                    )
                    
                    if result_bytes:
                        # Очищаем прогресс-бар и показываем успех
                        progress_container.empty()
                        st.success("✅ Файлы успешно объединены!")
                        
                        # Показываем статистику объединения
                        total_size_mb = len(result_bytes) / (1024 * 1024)
                        st.info(f"📊 Размер результирующего файла: {total_size_mb:.2f} MB")
                        
                        # Кнопка скачивания
                        st.download_button(
                            label="📥 Скачать объединенный файл",
                            data=result_bytes,
                            file_name="merged_file.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        )
                        
                        # Сбрасываем флаг после успешного завершения
                        st.session_state.is_merging = False
                    else:
                        progress_container.empty()
                        st.error("❌ Ошибка при объединении файлов")
                        # Сбрасываем флаг после ошибки
                        st.session_state.is_merging = False
                        
                except Exception as e:
                    progress_container.empty()
                    st.error(f"❌ Произошла ошибка: {str(e)}")
                    st.error("Попробуйте уменьшить размер файлов или количество файлов")
                    # Сбрасываем флаг после ошибки
                    st.session_state.is_merging = False
    else:
        st.warning("⚠️ Выберите хотя бы один лист для объединения")

# Справочная информация
with st.expander("ℹ️ Как использовать инструмент"):
    st.markdown("""
    ### 📝 Инструкция по использованию:
    
    1. **Загрузите файл-шаблон** - это основной файл, структура которого будет сохранена
    2. **Загрузите дополнительные файлы** - файлы, данные из которых нужно добавить
    3. **Настройте объединение**:
       - Выберите листы для объединения
       - Укажите с какой строки брать данные из дополнительных файлов
    4. **Выполните объединение** и скачайте результат
    
    ### 🖥️ Серверная обработка:
    
    - **🔒 Безопасность**: Все файлы обрабатываются на сервере приложения
    - **⚡ Производительность**: Использует ресурсы сервера для быстрой обработки
    - **📊 Прогресс**: Детальный прогресс-бар показывает каждый этап обработки
    - **💾 Временные файлы**: Автоматически удаляются после обработки
    - **📈 Масштабируемость**: Подходит для обработки больших файлов
    
    ### 🔧 Принцип работы:
    
    - **Необъединяемые листы** остаются как в оригинальном шаблоне
    - **Объединяемые листы** сохраняют структуру шаблона, но к ним добавляются данные из дополнительных файлов
    - **Начальная строка** определяет с какой строки брать данные (чтобы исключить заголовки)
    - **Прогресс-бар** показывает: загрузку, обработку каждого листа, запись данных, сохранение
    
    ### 💡 Пример работы прогресс-бара:
    
    ```
    📂 Загрузка файла-шаблона...           [10%]
    📋 Обработка листа 'Шаблон'...         [25%]
    📄 Обработка файла 1/3 для листа...    [40%]
    🔗 Объединение данных для листа...      [60%]
    ✏️ Запись данных: 500/1000 строк...    [80%]
    💾 Сохранение результирующего файла...  [95%]
    ✅ Объединение завершено!              [100%]
    ```
    
    ### 🎯 Оптимизация для больших файлов:
    
    - **Потоковая обработка**: Данные обрабатываются поэтапно
    - **Умный прогресс**: Более частые обновления для файлов >1000 строк  
    - **Управление памятью**: Временные файлы для снижения нагрузки на RAM
    - **Обработка ошибок**: Детальная диагностика при возникновении проблем
    """)

# Статистика в сайдбаре
with st.sidebar:
    st.subheader("📊 Статистика")
    
    # Показываем статус обработки
    if st.session_state.is_merging:
        st.error("⏳ Выполняется объединение файлов...")
        st.warning("🚫 Пожалуйста, не закрывайте страницу")
    
    if st.session_state.template_file:
        st.metric("📄 Шаблон", "Загружен")
        st.metric("🗂️ Листов в шаблоне", len(st.session_state.available_sheets))
    else:
        st.metric("📄 Шаблон", "Не загружен")
    
    st.metric("📁 Дополнительных файлов", len(st.session_state.additional_files))
    
    if st.session_state.sheet_config:
        merge_count = len([s for s, c in st.session_state.sheet_config.items() if c['merge']])
        st.metric("🔗 Листов для объединения", merge_count)
    
    # Кнопка сброса процесса (только если есть активный процесс)
    if st.session_state.is_merging:
        st.markdown("---")
        if st.button("🛑 Остановить процесс", type="secondary", use_container_width=True):
            st.session_state.is_merging = False
            st.rerun() 