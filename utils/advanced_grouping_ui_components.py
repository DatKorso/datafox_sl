"""UI компоненты для улучшенной страницы группировки товаров.

Этот модуль содержит переиспользуемые UI компоненты для страницы
улучшенной группировки товаров, следуя паттернам проекта.

Автор: DataFox SL Project
Версия: 2.0.0
"""

import streamlit as st
import pandas as pd
from typing import List, Dict, Any, Optional
from utils.advanced_product_grouper import GroupingConfig, GroupingResult


def render_grouping_configuration() -> GroupingConfig:
    """Отображает UI для настройки параметров группировки.
    
    Returns:
        GroupingConfig: Конфигурация группировки
    """
    st.subheader("⚙️ Настройки группировки")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Основные параметры:**")
        
        # Колонки для группировки
        available_columns = ['gender', 'wb_category', 'brand', 'color']
        grouping_columns = st.multiselect(
            "Колонки для группировки",
            options=available_columns,
            default=['gender', 'wb_category'],
            help="Товары будут группироваться по выбранным параметрам"
        )
        
        # Минимальный рейтинг группы
        min_group_rating = st.number_input(
            "Минимальный рейтинг группы",
            min_value=0.0,
            max_value=5.0,
            value=4.0,
            step=0.1,
            help="Группы с рейтингом ниже этого значения будут дополнены компенсаторами"
        )
        
        # Максимальное количество SKU в группе
        max_wb_sku_per_group = st.number_input(
            "Максимальное количество SKU в группе",
            min_value=1,
            max_value=50,
            value=10,
            help="Максимальное количество товаров в одной группе"
        )
    
    with col2:
        st.markdown("**Дополнительные опции:**")
        
        # Включить приоритет по полю sort
        enable_sort_priority = st.checkbox(
            "Использовать приоритет по полю 'sort'",
            value=True,
            help="Товары будут обрабатываться в порядке приоритета из поля sort"
        )
        
        # Фильтр по категории
        wb_category = st.text_input(
            "Фильтр по категории WB (опционально)",
            value="",
            help="Оставьте пустым для обработки всех категорий"
        )
        
        if wb_category.strip() == "":
            wb_category = None
    
    # Валидация
    if not grouping_columns:
        st.error("❌ Необходимо выбрать хотя бы одну колонку для группировки")
        st.stop()
    
    return GroupingConfig(
        grouping_columns=grouping_columns,
        min_group_rating=min_group_rating,
        max_wb_sku_per_group=max_wb_sku_per_group,
        enable_sort_priority=enable_sort_priority,
        wb_category=wb_category
    )


def render_wb_sku_input() -> List[str]:
    """Отображает UI для ввода WB SKU.
    
    Returns:
        List[str]: Список WB SKU для обработки
    """
    st.subheader("📝 Ввод WB SKU")
    
    input_method = st.radio(
        "Способ ввода WB SKU:",
        options=["Текстовое поле", "Загрузка файла"],
        horizontal=True
    )
    
    wb_skus = []
    
    if input_method == "Текстовое поле":
        wb_sku_text = st.text_area(
            "Введите WB SKU (по одному на строку):",
            height=150,
            help="Каждый WB SKU должен быть на отдельной строке"
        )
        
        if wb_sku_text.strip():
            wb_skus = [sku.strip() for sku in wb_sku_text.strip().split('\n') if sku.strip()]
    
    else:  # Загрузка файла
        uploaded_file = st.file_uploader(
            "Загрузите файл с WB SKU",
            type=['txt', 'csv', 'xlsx'],
            help="Файл должен содержать WB SKU в первой колонке"
        )
        
        if uploaded_file is not None:
            try:
                if uploaded_file.name.endswith('.txt'):
                    content = uploaded_file.read().decode('utf-8')
                    wb_skus = [sku.strip() for sku in content.split('\n') if sku.strip()]
                
                elif uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                    wb_skus = df.iloc[:, 0].astype(str).tolist()
                
                elif uploaded_file.name.endswith('.xlsx'):
                    df = pd.read_excel(uploaded_file)
                    wb_skus = df.iloc[:, 0].astype(str).tolist()
                
                # Очищаем от пустых значений
                wb_skus = [sku.strip() for sku in wb_skus if str(sku).strip() and str(sku).strip() != 'nan']
                
            except Exception as e:
                st.error(f"❌ Ошибка при чтении файла: {str(e)}")
                wb_skus = []
    
    # Показываем количество найденных SKU
    if wb_skus:
        st.success(f"✅ Найдено {len(wb_skus)} WB SKU для обработки")
        
        # Показываем первые несколько для проверки
        with st.expander("Предварительный просмотр WB SKU"):
            preview_count = min(10, len(wb_skus))
            st.write(f"Первые {preview_count} из {len(wb_skus)} WB SKU:")
            for i, sku in enumerate(wb_skus[:preview_count], 1):
                st.write(f"{i}. {sku}")
            
            if len(wb_skus) > preview_count:
                st.write(f"... и еще {len(wb_skus) - preview_count} SKU")
    
    return wb_skus


def render_grouping_results(result: GroupingResult):
    """Отображает результаты группировки.
    
    Args:
        result: Результат группировки товаров
    """
    st.subheader("📊 Результаты группировки")
    
    # Статистика
    render_grouping_statistics(result.statistics)
    
    # Логи процесса
    render_process_logs(result.logs)
    
    # Группы товаров
    render_product_groups(result.groups)
    
    # Проблемные товары
    render_problematic_items(result.low_rating_items, result.defective_items)


def render_grouping_statistics(statistics: Dict[str, Any]):
    """Отображает статистику группировки.
    
    Args:
        statistics: Словарь со статистикой
    """
    st.markdown("### 📈 Статистика")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Всего групп",
            statistics.get('total_groups', 0)
        )
    
    with col2:
        st.metric(
            "Обработано товаров",
            statistics.get('total_items_processed', 0)
        )
    
    with col3:
        avg_size = statistics.get('avg_group_size', 0)
        st.metric(
            "Средний размер группы",
            f"{avg_size:.1f}"
        )
    
    with col4:
        avg_rating = statistics.get('avg_group_rating', 0)
        st.metric(
            "Средний рейтинг групп",
            f"{avg_rating:.2f}"
        )
    
    # Дополнительная статистика
    if statistics.get('low_rating_items_count', 0) > 0 or statistics.get('defective_items_count', 0) > 0:
        st.markdown("**Проблемные товары:**")
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric(
                "Товары с низким рейтингом",
                statistics.get('low_rating_items_count', 0)
            )
        
        with col2:
            st.metric(
                "Дефектные товары",
                statistics.get('defective_items_count', 0)
            )


def render_process_logs(logs: List[str]):
    """Отображает логи процесса группировки.
    
    Args:
        logs: Список сообщений лога
    """
    with st.expander("📋 Логи процесса группировки", expanded=False):
        if logs:
            for log_entry in logs:
                if "[ERROR]" in log_entry:
                    st.error(log_entry)
                elif "[WARNING]" in log_entry:
                    st.warning(log_entry)
                else:
                    st.info(log_entry)
        else:
            st.info("Логи отсутствуют")


def render_product_groups(groups: List[Dict[str, Any]]):
    """Отображает созданные группы товаров.
    
    Args:
        groups: Список групп товаров
    """
    st.markdown("### 🎯 Созданные группы")
    
    if not groups:
        st.warning("Группы не созданы")
        return
    
    # Фильтры для групп
    col1, col2 = st.columns(2)
    
    with col1:
        min_rating_filter = st.number_input(
            "Минимальный рейтинг для отображения",
            min_value=0.0,
            max_value=5.0,
            value=0.0,
            step=0.1,
            key="group_rating_filter"
        )
    
    with col2:
        min_size_filter = st.number_input(
            "Минимальный размер группы",
            min_value=1,
            max_value=50,
            value=1,
            key="group_size_filter"
        )
    
    # Фильтруем группы
    filtered_groups = [
        group for group in groups
        if group['group_rating'] >= min_rating_filter and
           group['item_count'] >= min_size_filter
    ]
    
    st.write(f"Показано {len(filtered_groups)} из {len(groups)} групп")
    
    # Отображаем группы
    for i, group in enumerate(filtered_groups, 1):
        with st.expander(
            f"Группа {group['group_id']}: {group['item_count']} товаров, рейтинг {group['group_rating']:.2f}",
            expanded=False
        ):
            # Отображаем информацию о группе
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Рейтинг группы", f"{group['group_rating']:.2f}")
            with col2:
                st.metric("Количество товаров", group['item_count'])
            with col3:
                priority_count = sum(1 for item in group['items'] if item.get('is_priority_item', False))
                st.metric("Приоритетных товаров", priority_count)
            
            # Создаем DataFrame для отображения
            items_df = pd.DataFrame(group['items'])
            

            # Выбираем колонки для отображения
            display_columns = ['wb_sku', 'avg_rating', 'total_stock', 'is_priority_item']
            if 'gender' in items_df.columns:
                display_columns.append('gender')
            if 'wb_category' in items_df.columns:
                display_columns.append('wb_category')
            if 'brand' in items_df.columns:
                display_columns.append('brand')
            if 'color' in items_df.columns:
                display_columns.append('color')
            
            # Фильтруем существующие колонки
            available_columns = [col for col in display_columns if col in items_df.columns]
            
            if available_columns:
                # Подготавливаем данные для отображения
                display_df = items_df[available_columns].copy()
                
                # Сохраняем оригинальную колонку для стилизации
                original_priority = display_df['is_priority_item'].copy() if 'is_priority_item' in display_df.columns else None
                
                # Переименовываем колонки для лучшего отображения
                column_names = {
                    'wb_sku': 'WB SKU',
                    'avg_rating': 'Рейтинг',
                    'total_stock': 'Остаток',
                    'is_priority_item': 'Приоритетный',
                    'gender': 'Пол',
                    'wb_category': 'Категория',
                    'brand': 'Бренд',
                    'color': 'Цвет'
                }
                display_df = display_df.rename(columns=column_names)
                
                # Форматируем колонку приоритетности
                if 'Приоритетный' in display_df.columns:
                    display_df['Приоритетный'] = display_df['Приоритетный'].astype(bool).map({True: '✅ Да', False: '❌ Нет'})
                
                # Создаем функцию стилизации с использованием оригинальных данных
                def highlight_priority_items(row):
                    if original_priority is not None:
                        row_index = row.name
                        # Проверяем, есть ли этот индекс в оригинальных данных
                        if row_index in original_priority.index and original_priority.loc[row_index]:
                            return ['background-color: #ffcdd2; color: #d32f2f; font-weight: bold'] * len(row)
                    return [''] * len(row)
                
                # Применяем стилизацию
                styled_df = display_df.style.apply(highlight_priority_items, axis=1)
                
                st.dataframe(
                    styled_df,
                    use_container_width=True,
                    hide_index=True
                )
                
                # Дополнительная информация о группе
                st.markdown("**Легенда:**")
                st.markdown("🔴 **Красный фон** - приоритетные товары (сезон 13 или положительный остаток)")
                st.markdown("⚪ **Обычный фон** - компенсаторы (неприоритетные товары с высоким рейтингом)")
            else:
                st.write("Данные для отображения недоступны")


def render_problematic_items(
    low_rating_items: List[Dict[str, Any]], 
    defective_items: List[Dict[str, Any]]
):
    """Отображает проблемные товары.
    
    Args:
        low_rating_items: Товары с низким рейтингом
        defective_items: Дефектные товары
    """
    if not low_rating_items and not defective_items:
        return
    
    st.markdown("### ⚠️ Проблемные товары")
    
    if low_rating_items:
        with st.expander(f"Товары с низким рейтингом ({len(low_rating_items)})", expanded=False):
            df = pd.DataFrame(low_rating_items)
            display_columns = ['wb_sku', 'avg_rating', 'is_priority_item']
            if 'gender' in df.columns:
                display_columns.append('gender')
            if 'wb_category' in df.columns:
                display_columns.append('wb_category')
            if 'total_stock' in df.columns:
                display_columns.append('total_stock')
            
            available_columns = [col for col in display_columns if col in df.columns]
            if available_columns:
                # Подготавливаем данные для отображения
                display_df = df[available_columns].copy()
                
                # Переименовываем колонки
                column_names = {
                    'wb_sku': 'WB SKU',
                    'avg_rating': 'Рейтинг',
                    'total_stock': 'Остаток',
                    'is_priority_item': 'Приоритетный',
                    'gender': 'Пол',
                    'wb_category': 'Категория'
                }
                display_df = display_df.rename(columns=column_names)
                
                # Форматируем колонку приоритетности
                if 'Приоритетный' in display_df.columns:
                    display_df['Приоритетный'] = display_df['Приоритетный'].map({True: '✅ Да', False: '❌ Нет'})
                
                st.dataframe(display_df, use_container_width=True, hide_index=True)
                
                # Показываем статистику
                priority_count = sum(1 for item in low_rating_items if item.get('is_priority_item', False))
                st.info(f"Из них приоритетных товаров: {priority_count}")
    
    if defective_items:
        with st.expander(f"Дефектные товары ({len(defective_items)})", expanded=False):
            df = pd.DataFrame(defective_items)
            display_columns = ['wb_sku', 'avg_rating', 'is_priority_item']
            if 'gender' in df.columns:
                display_columns.append('gender')
            if 'wb_category' in df.columns:
                display_columns.append('wb_category')
            if 'total_stock' in df.columns:
                display_columns.append('total_stock')
            
            available_columns = [col for col in display_columns if col in df.columns]
            if available_columns:
                # Подготавливаем данные для отображения
                display_df = df[available_columns].copy()
                
                # Переименовываем колонки
                column_names = {
                    'wb_sku': 'WB SKU',
                    'avg_rating': 'Рейтинг',
                    'total_stock': 'Остаток',
                    'is_priority_item': 'Приоритетный',
                    'gender': 'Пол',
                    'wb_category': 'Категория'
                }
                display_df = display_df.rename(columns=column_names)
                
                # Форматируем колонку приоритетности
                if 'Приоритетный' in display_df.columns:
                    display_df['Приоритетный'] = display_df['Приоритетный'].map({True: '✅ Да', False: '❌ Нет'})
                
                st.dataframe(display_df, use_container_width=True, hide_index=True)
                
                # Показываем статистику
                priority_count = sum(1 for item in defective_items if item.get('is_priority_item', False))
                st.info(f"Из них приоритетных товаров: {priority_count}")


def render_export_options(result: GroupingResult):
    """Отображает опции экспорта результатов.
    
    Args:
        result: Результат группировки
    """
    st.subheader("💾 Экспорт результатов")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📊 Экспорт в Excel", use_container_width=True):
            excel_data = export_to_excel(result)
            if excel_data:
                st.download_button(
                    label="⬇️ Скачать Excel файл",
                    data=excel_data,
                    file_name=f"product_groups_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
    
    with col2:
        if st.button("📋 Экспорт в CSV", use_container_width=True):
            csv_data = export_to_csv(result)
            if csv_data:
                st.download_button(
                    label="⬇️ Скачать CSV файл",
                    data=csv_data,
                    file_name=f"product_groups_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )


def export_to_excel(result: GroupingResult) -> Optional[bytes]:
    """Экспортирует результаты в Excel формат.
    
    Args:
        result: Результат группировки
        
    Returns:
        bytes: Данные Excel файла или None при ошибке
    """
    try:
        import io
        from openpyxl import Workbook
        
        wb = Workbook()
        
        # Лист со статистикой
        ws_stats = wb.active
        ws_stats.title = "Статистика"
        
        stats_data = [
            ["Параметр", "Значение"],
            ["Всего групп", result.statistics.get('total_groups', 0)],
            ["Обработано товаров", result.statistics.get('total_items_processed', 0)],
            ["Средний размер группы", f"{result.statistics.get('avg_group_size', 0):.1f}"],
            ["Средний рейтинг групп", f"{result.statistics.get('avg_group_rating', 0):.2f}"],
            ["Товары с низким рейтингом", result.statistics.get('low_rating_items_count', 0)],
            ["Дефектные товары", result.statistics.get('defective_items_count', 0)]
        ]
        
        for row in stats_data:
            ws_stats.append(row)
        
        # Лист с группами
        ws_groups = wb.create_sheet("Группы")
        
        # Заголовки
        headers = ["Группа", "WB SKU", "Рейтинг", "Остатки", "Пол", "Категория"]
        ws_groups.append(headers)
        
        # Данные групп
        for group in result.groups:
            for item in group['items']:
                row = [
                    group['group_id'],
                    item.get('wb_sku', ''),
                    item.get('avg_rating', ''),
                    item.get('total_stock', ''),
                    item.get('gender', ''),
                    item.get('wb_category', '')
                ]
                ws_groups.append(row)
        
        # Сохраняем в байты
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)
        
        return output.getvalue()
        
    except Exception as e:
        st.error(f"Ошибка при экспорте в Excel: {str(e)}")
        return None


def export_to_csv(result: GroupingResult) -> Optional[str]:
    """Экспортирует результаты в CSV формат.
    
    Args:
        result: Результат группировки
        
    Returns:
        str: Данные CSV файла или None при ошибке
    """
    try:
        rows = []
        
        # Заголовки
        headers = ["Группа", "WB SKU", "Рейтинг", "Остатки", "Пол", "Категория"]
        rows.append(headers)
        
        # Данные групп
        for group in result.groups:
            for item in group['items']:
                row = [
                    group['group_id'],
                    item.get('wb_sku', ''),
                    item.get('avg_rating', ''),
                    item.get('total_stock', ''),
                    item.get('gender', ''),
                    item.get('wb_category', '')
                ]
                rows.append(row)
        
        # Преобразуем в CSV
        import csv
        import io
        
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerows(rows)
        
        return output.getvalue()
        
    except Exception as e:
        st.error(f"Ошибка при экспорте в CSV: {str(e)}")
        return None


def render_help_section():
    """Отображает раздел помощи."""
    with st.expander("❓ Справка по использованию", expanded=False):
        st.markdown("""
        ### Как использовать улучшенную группировку товаров:
        
        1. **Настройка параметров:**
           - Выберите колонки для группировки (товары с одинаковыми значениями будут объединены)
           - Установите минимальный рейтинг группы (группы с низким рейтингом будут дополнены компенсаторами)
           - Укажите максимальное количество SKU в группе
        
        2. **Ввод данных:**
           - Введите WB SKU через текстовое поле или загрузите файл
           - Поддерживаются форматы: TXT, CSV, XLSX
        
        3. **Запуск группировки:**
           - Нажмите кнопку "Создать группы"
           - Дождитесь завершения обработки
        
        4. **Анализ результатов:**
           - Изучите статистику группировки
           - Просмотрите созданные группы
           - Проверьте проблемные товары
        
        5. **Экспорт:**
           - Экспортируйте результаты в Excel или CSV для дальнейшей работы
        
        ### Улучшения по сравнению с оригинальной версией:
        
        - ✅ Исправлены SQL запросы для поиска компенсаторов
        - ✅ Справедливое распределение компенсаторов между группами
        - ✅ Детальное логирование процесса
        - ✅ Улучшенная архитектура кода
        - ✅ Расширенные возможности экспорта
        """)