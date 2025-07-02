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
    """Объединить Excel файлы согласно конфигурации с прогресс-баром для облачного сервера"""
    tmp_template = None
    try:
        if progress_callback:
            progress_callback(0.1, "🔍 Анализ конфигурации...")
        
        # Подсчитываем общее количество операций для прогресса
        merge_sheets = [name for name, config in sheet_config.items() if config['merge']]
        total_operations = len(merge_sheets) * (len(additional_files_bytes) + 2)
        current_operation = 0
        
        def update_progress(message):
            nonlocal current_operation
            current_operation += 1
            if progress_callback:
                progress_callback(min(current_operation / total_operations, 0.95), message)
        
        update_progress("📂 Загрузка файла-шаблона...")
        
        # Создаем временный файл более безопасным способом для облачного сервера
        try:
            tmp_template = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False, mode='wb')
            tmp_template.write(template_bytes)
            tmp_template.close()  # Закрываем файл перед использованием
            
            # Проверяем, что файл действительно создался
            if not os.path.exists(tmp_template.name):
                raise Exception("Не удалось создать временный файл шаблона")
            
            if progress_callback:
                progress_callback(0.2, "🔧 Загрузка Excel шаблона...")
            
            template_wb = load_workbook(tmp_template.name)
            
        except Exception as e:
            raise Exception(f"Ошибка при создании/загрузке временного файла: {str(e)}")
        
        # Для каждого листа в конфигурации
        for sheet_idx, (sheet_name, config) in enumerate(sheet_config.items()):
            if config['merge'] and sheet_name in template_wb.sheetnames:
                start_row = config['start_row']
                
                update_progress(f"📋 Обработка листа '{sheet_name}'...")
                
                try:
                    # Читаем исходный лист шаблона
                    template_df = pd.read_excel(tmp_template.name, sheet_name=sheet_name, header=None)
                    
                    # Собираем данные из дополнительных файлов
                    additional_data = []
                    for file_idx, add_file_bytes in enumerate(additional_files_bytes):
                        update_progress(f"📄 Обработка файла {file_idx + 1}/{len(additional_files_bytes)} для листа '{sheet_name}'...")
                        
                        try:
                            add_df = read_excel_sheet(add_file_bytes, sheet_name, start_row)
                            if not add_df.empty:
                                additional_data.append(add_df)
                        except Exception as e:
                            # Пропускаем файлы с ошибками, но логируем
                            if progress_callback:
                                progress_callback(
                                    current_operation / total_operations,
                                    f"⚠️ Пропуск файла {file_idx + 1} (ошибка): {str(e)[:50]}..."
                                )
                    
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
                            
                            # Обновляем прогресс каждые 500 строк (реже для облачного сервера)
                            if total_rows > 1000 and r_idx % 500 == 0 and progress_callback:
                                write_progress = (r_idx / total_rows) * 0.1
                                progress_callback(
                                    min((current_operation + write_progress) / total_operations, 0.9),
                                    f"✏️ Запись данных: {r_idx + 1}/{total_rows} строк в лист '{sheet_name}'"
                                )
                        
                        if progress_callback:
                            progress_callback(
                                min(current_operation / total_operations, 0.9),
                                f"✅ Лист '{sheet_name}' обработан ({total_rows} строк)"
                            )
                
                except Exception as e:
                    # Логируем ошибку листа, но продолжаем обработку
                    if progress_callback:
                        progress_callback(
                            current_operation / total_operations,
                            f"❌ Ошибка листа '{sheet_name}': {str(e)[:50]}..."
                        )
        
        if progress_callback:
            progress_callback(0.95, "💾 Сохранение результирующего файла...")
        
        # Сохраняем результат
        output = BytesIO()
        template_wb.save(output)
        output.seek(0)
        
        template_wb.close()
        
        if progress_callback:
            progress_callback(1.0, "✅ Объединение завершено!")
        
        return output.getvalue()
        
    except Exception as e:
        error_msg = f"Критическая ошибка при объединении файлов: {str(e)}"
        if progress_callback:
            progress_callback(0, f"❌ {error_msg}")
        raise Exception(error_msg)
        
    finally:
        # Гарантированная очистка временного файла
        if tmp_template and hasattr(tmp_template, 'name') and os.path.exists(tmp_template.name):
            try:
                os.unlink(tmp_template.name)
            except Exception:
                pass  # Игнорируем ошибки удаления в облачной среде

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
            # Простая логика без флагов состояния для облачного сервера
            if st.button("🔗 Объединить файлы", type="primary", use_container_width=True):
                # Создаем контейнеры для прогресс-бара
                progress_container = st.container()
                
                with progress_container:
                    # Показываем начальную информацию
                    merge_sheets_count = len([s for s, c in st.session_state.sheet_config.items() if c['merge']])
                    additional_files_count = len(st.session_state.additional_files)
                    
                    st.info(f"🔄 Начинается объединение: {merge_sheets_count} листов × {additional_files_count + 1} файлов")
                    st.warning("⚠️ Не закрывайте страницу до завершения процесса!")
                    
                    # Создаем прогресс бар
                    progress_bar = st.progress(0)
                    progress_text = st.empty()
                    
                    # Callback функция для обновления прогресса
                    def update_progress(value, message):
                        progress_bar.progress(min(value, 1.0))
                        progress_text.text(message)
                    
                    try:
                        # Выполняем объединение с прогресс-баром
                        result_bytes = merge_excel_files(
                            st.session_state.template_file,
                            st.session_state.additional_files,
                            st.session_state.sheet_config,
                            progress_callback=update_progress
                        )
                        
                        if result_bytes:
                            # Очищаем прогресс-бар и показываем успех
                            progress_bar.empty()
                            progress_text.empty()
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
                        else:
                            progress_bar.empty()
                            progress_text.empty()
                            st.error("❌ Ошибка при объединении файлов")
                            
                    except Exception as e:
                        progress_bar.empty()
                        progress_text.empty()
                        st.error(f"❌ Произошла ошибка: {str(e)}")
                        st.error("Попробуйте уменьшить размер файлов или количество файлов")
                        # Показываем подробную ошибку для отладки
                        with st.expander("🔍 Подробности ошибки"):
                            st.code(traceback.format_exc())
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
    
    if st.session_state.template_file:
        st.metric("📄 Шаблон", "Загружен")
        st.metric("🗂️ Листов в шаблоне", len(st.session_state.available_sheets))
    else:
        st.metric("📄 Шаблон", "Не загружен")
    
    st.metric("📁 Дополнительных файлов", len(st.session_state.additional_files))
    
    if st.session_state.sheet_config:
        merge_count = len([s for s, c in st.session_state.sheet_config.items() if c['merge']])
        st.metric("🔗 Листов для объединения", merge_count)
    
    # Информация о готовности к объединению
    st.markdown("---")
    if (st.session_state.template_file and 
        st.session_state.additional_files and 
        st.session_state.sheet_config and
        any(config['merge'] for config in st.session_state.sheet_config.values())):
        st.success("✅ Готово к объединению")
    else:
        st.warning("⚠️ Настройте параметры")
    
    # Системная информация для отладки
    with st.expander("🔧 Система"):
        st.text(f"Streamlit: {st.__version__}")
        st.text(f"Platform: Cloud Server")
        
        # Тест записи в temp директорию
        try:
            test_file = tempfile.NamedTemporaryFile(delete=True)
            test_file.close()
            st.success("✅ Temp файлы: OK")
        except Exception as e:
            st.error(f"❌ Temp файлы: {str(e)}") 