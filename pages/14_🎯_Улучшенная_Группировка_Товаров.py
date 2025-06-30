"""Страница улучшенной группировки товаров.

Эта страница предоставляет исправленный функционал группировки товаров
с корректной компенсацией рейтинга и улучшенной архитектурой.

Основные улучшения:
- Исправлены SQL запросы для поиска компенсаторов
- Справедливое распределение компенсаторов между группами
- Детальное логирование процесса
- Разделение бизнес-логики и UI
- Расширенные возможности экспорта
- 🆕 Отображение фотографий товаров в таблицах

Автор: DataFox SL Project
Версия: 2.1.2
"""

import streamlit as st
import pandas as pd
from typing import List, Optional
import traceback

# Импорты проекта
from utils.db_connection import connect_db
from utils.advanced_product_grouper import AdvancedProductGrouper, GroupingConfig
from utils.advanced_grouping_ui_components import (
    render_grouping_configuration,
    render_wb_sku_input,
    render_grouping_results,
    render_export_options,
    render_help_section
)

# Настройка страницы
st.set_page_config(
    page_title="Улучшенная группировка товаров",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Заголовок страницы
st.title("🎯 Улучшенная группировка товаров")
st.markdown("""
Эта страница предоставляет исправленный функционал группировки товаров 
с корректной компенсацией рейтинга и улучшенной архитектурой.

**Новое**: Теперь в таблицах отображаются фотографии товаров! 📸
""")

# Проверка подключения к базе данных
try:
    conn = connect_db()
    if conn is None:
        st.error("❌ Не удалось подключиться к базе данных. Перейдите в настройки для настройки подключения.")
        st.stop()
except Exception as e:
    st.error(f"❌ Ошибка подключения к базе данных: {str(e)}")
    st.stop()

# Боковая панель с информацией
with st.sidebar:
    st.markdown("### 📋 Информация")
    st.markdown("""
    **Версия:** 2.1.2
    
    **Основные улучшения:**
    - ✅ Исправлены SQL запросы
    - ✅ Справедливое распределение компенсаторов
    - ✅ Детальное логирование
    - ✅ Улучшенная архитектура
    - ✅ Расширенный экспорт
    - 🆕 **Фотографии товаров в таблицах**
    - 🔧 **Исправлена логика группировки приоритетных товаров**
    """)
    
    st.markdown("### 🔧 Статус системы")
    st.success("✅ База данных подключена")
    
    # Справка
    render_help_section()

# Основной интерфейс
tab1, tab2 = st.tabs(["🎯 Группировка", "📊 Результаты"])

with tab1:
    # Настройки группировки
    config = render_grouping_configuration()
    
    st.divider()
    
    # Ввод WB SKU
    wb_skus = render_wb_sku_input()
    
    st.divider()
    
    # Кнопка запуска группировки
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        if st.button(
            "🚀 Создать группы",
            type="primary",
            use_container_width=True,
            disabled=len(wb_skus) == 0
        ):
            if len(wb_skus) == 0:
                st.error("❌ Необходимо ввести WB SKU для группировки")
            else:
                # Запускаем группировку
                with st.spinner("Выполняется группировка товаров..."):
                    try:
                        # Создаем экземпляр группировщика
                        grouper = AdvancedProductGrouper(conn)
                        
                        # Выполняем группировку
                        result = grouper.create_advanced_product_groups(wb_skus, config)
                        
                        # Сохраняем результат в session_state
                        st.session_state['grouping_result'] = result
                        st.session_state['grouping_config'] = config
                        
                        # Показываем краткую статистику
                        st.success(f"✅ Группировка завершена! Создано {result.statistics.get('total_groups', 0)} групп")
                        
                        # Переключаемся на вкладку результатов
                        st.info("📊 Перейдите на вкладку 'Результаты' для просмотра детальной информации")
                        
                    except Exception as e:
                        st.error(f"❌ Ошибка при выполнении группировки: {str(e)}")
                        
                        # Показываем детальную информацию об ошибке в expander
                        with st.expander("🔍 Детали ошибки", expanded=False):
                            st.code(traceback.format_exc())

with tab2:
    # Проверяем наличие результатов группировки
    if 'grouping_result' not in st.session_state:
        st.info("📋 Результаты группировки отсутствуют. Выполните группировку на вкладке 'Группировка'.")
    else:
        result = st.session_state['grouping_result']
        config = st.session_state.get('grouping_config')
        
        # Показываем информацию о конфигурации
        if config:
            with st.expander("⚙️ Параметры группировки", expanded=False):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Колонки группировки:** {', '.join(config.grouping_columns)}")
                    st.write(f"**Минимальный рейтинг:** {config.min_group_rating}")
                
                with col2:
                    st.write(f"**Максимум SKU в группе:** {config.max_wb_sku_per_group}")
                    st.write(f"**Приоритет по sort:** {'Да' if config.enable_sort_priority else 'Нет'}")
                
                if config.wb_category:
                    st.write(f"**Фильтр по категории:** {config.wb_category}")
        
        st.divider()
        
        # Отображаем результаты
        render_grouping_results(result)
        
        st.divider()
        
        # Опции экспорта
        render_export_options(result)
        
        # Кнопка очистки результатов
        st.divider()
        
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col2:
            if st.button(
                "🗑️ Очистить результаты",
                use_container_width=True,
                help="Удалить текущие результаты группировки"
            ):
                if 'grouping_result' in st.session_state:
                    del st.session_state['grouping_result']
                if 'grouping_config' in st.session_state:
                    del st.session_state['grouping_config']
                
                st.success("✅ Результаты очищены")
                st.rerun()

# Футер страницы
st.divider()
st.markdown("""
<div style='text-align: center; color: #666; font-size: 0.8em;'>
    DataFox SL - Улучшенная группировка товаров v2.0.0<br>
    Исправленная версия с корректной компенсацией рейтинга
</div>
""", unsafe_allow_html=True)

# Закрываем соединение с базой данных
if 'conn' in locals() and conn:
    conn.close()