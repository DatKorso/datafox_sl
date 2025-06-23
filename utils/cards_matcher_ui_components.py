"""
UI Components for Cards Matcher functionality.

This module provides reusable UI components to reduce code duplication
in the Cards Matcher page while maintaining full functionality.

Created as part of code organization improvements.
"""
import streamlit as st
import pandas as pd
import os
from typing import Optional, Tuple, Dict, Any, List
from utils.config_utils import get_report_path, set_report_path


def render_brand_filter_info(brands_filter: str) -> None:
    """
    Отображение информации о фильтре брендов.
    
    Args:
        brands_filter: Строка с брендами, разделенными точкой с запятой
    """
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


def render_file_selector_component(
    config_key: str,
    file_description: str,
    file_types: List[str] = ['xlsx'],
    key_prefix: str = "",
    help_text: str = ""
) -> Optional[str]:
    """
    Универсальный компонент выбора файла.
    
    Args:
        config_key: Ключ конфигурации для пути к файлу
        file_description: Описание типа файла для пользователя
        file_types: Список допустимых расширений файлов
        key_prefix: Префикс для уникальности ключей Streamlit
        help_text: Дополнительный текст помощи
        
    Returns:
        Путь к выбранному файлу или None
    """
    st.subheader("📁 Выбор файла")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Get default path from settings
        default_path = get_report_path(config_key)
        
        # File selection method
        file_source = st.radio(
            "Как выбрать файл:",
            ["📂 Использовать путь из настроек", "⬆️ Загрузить файл"],
            index=0 if default_path else 1,
            key=f"{key_prefix}_file_source"
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
                f"Введите путь к файлу {file_description}:",
                placeholder=f"/path/to/{file_description.lower().replace(' ', '_')}.{file_types[0]}",
                help=f"Укажите полный путь к файлу .{file_types[0]} {help_text}",
                key=f"{key_prefix}_new_path"
            )
            
            if st.button("💾 Сохранить путь", key=f"{key_prefix}_save_path"):
                if new_path and new_path.strip():
                    try:
                        set_report_path(config_key, new_path.strip())
                        st.success("✅ Путь сохранен в настройках!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Ошибка сохранения пути: {e}")
                else:
                    st.warning("⚠️ Введите корректный путь к файлу")
    
    else:  # Upload file
        uploaded_file = st.file_uploader(
            f"Выберите файл {file_description}:",
            type=file_types,
            help=help_text,
            key=f"{key_prefix}_file_uploader"
        )
        
        if uploaded_file is not None:
            # Save uploaded file temporarily
            temp_path = f"temp_{key_prefix}_{uploaded_file.name}"
            with open(temp_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            selected_file_path = temp_path
            st.success(f"✅ Файл загружен: `{uploaded_file.name}`")
    
    return selected_file_path


def render_statistics_metrics(stats: Dict[str, Any], title: str = "📈 Статистика") -> None:
    """
    Отображение метрик статистики в едином формате.
    
    Args:
        stats: Словарь со статистическими данными
        title: Заголовок секции статистики
    """
    if not stats:
        st.info("📭 Статистические данные недоступны")
        return
    
    st.subheader(title)
    
    # Основные метрики в 4 колонки
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if 'total_products' in stats:
            st.metric("📊 Записей", stats['total_products'])
        elif 'total_records' in stats:
            st.metric("📊 Записей", stats['total_records'])
    
    with col2:
        if 'unique_skus' in stats:
            st.metric("🏷️ Уникальных SKU", stats['unique_skus'])
        elif 'unique_vendor_codes' in stats:
            st.metric("🏷️ Уникальных кодов", stats['unique_vendor_codes'])
    
    with col3:
        if 'avg_rating' in stats:
            avg_rating = stats['avg_rating']
            st.metric("⭐ Средний рейтинг", f"{avg_rating:.2f}" if avg_rating else "N/A")
        elif 'total_groups' in stats:
            st.metric("🔗 Всего групп", stats['total_groups'])
    
    with col4:
        if 'total_reviews' in stats:
            st.metric("💬 Всего отзывов", stats['total_reviews'] if stats['total_reviews'] else 0)
        elif 'groups_with_2_plus' in stats:
            st.metric("👥 Групп с 2+ товарами", stats['groups_with_2_plus'])


def render_import_process_info() -> None:
    """
    Отображение информации о процессе импорта.
    """
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


def render_file_requirements_info() -> None:
    """
    Отображение требований к файлу с данными.
    """
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


def render_quick_data_preview(df: pd.DataFrame, title: str = "👀 Предпросмотр данных") -> None:
    """
    Отображение быстрого предпросмотра данных.
    
    Args:
        df: DataFrame для предпросмотра
        title: Заголовок секции
    """
    if df.empty:
        st.warning("⚠️ Нет данных для предпросмотра")
        return
    
    with st.expander(f"{title} (первые 10 строк)"):
        st.dataframe(df.head(10), use_container_width=True, hide_index=True)


def render_export_controls(df: pd.DataFrame, filename_prefix: str, 
                          description: str = "результаты") -> None:
    """
    Отображение контролов экспорта данных.
    
    Args:
        df: DataFrame для экспорта
        filename_prefix: Префикс имени файла
        description: Описание экспортируемых данных
    """
    if df.empty:
        st.info(f"📭 Нет данных для экспорта {description}")
        return
    
    if st.button(f"📥 Экспортировать {description} в Excel", 
                key=f"export_{filename_prefix}"):
        try:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{filename_prefix}_{timestamp}.xlsx"
            
            df.to_excel(filename, index=False)
            
            with open(filename, "rb") as file:
                st.download_button(
                    label="📩 Скачать файл Excel",
                    data=file.read(),
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"download_{filename_prefix}"
                )
                
            # Cleanup temp file
            try:
                os.remove(filename)
            except:
                pass
                
        except Exception as e:
            st.error(f"❌ Ошибка при экспорте: {e}")


def render_error_message(error: Exception, context: str = "") -> None:
    """
    Стандартизированное отображение ошибок.
    
    Args:
        error: Исключение для отображения
        context: Контекст возникновения ошибки
    """
    error_msg = f"❌ Ошибка"
    if context:
        error_msg += f" {context}"
    error_msg += f": {str(error)}"
    
    st.error(error_msg)
    
    # Дополнительная информация об ошибке в expander
    with st.expander("🔍 Детали ошибки"):
        st.code(str(error))
        if context:
            st.info(f"**Контекст**: {context}")


def render_success_message(message: str, details: Optional[Dict[str, Any]] = None) -> None:
    """
    Стандартизированное отображение успешных операций.
    
    Args:
        message: Основное сообщение об успехе
        details: Дополнительные детали для отображения
    """
    st.success(f"✅ {message}")
    
    if details:
        cols = st.columns(len(details))
        for i, (key, value) in enumerate(details.items()):
            with cols[i]:
                st.metric(key, value) 