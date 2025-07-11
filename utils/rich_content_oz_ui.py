"""
UI компоненты для работы с Rich Content Ozon.

Этот модуль содержит переиспользуемые компоненты Streamlit для:
- Выбора и фильтрации товаров
- Конфигурации алгоритма рекомендаций
- Превью Rich Content JSON
- Отображения статистики и результатов
"""

import streamlit as st
import pandas as pd
import json
from typing import List, Dict, Any, Optional, Tuple
import plotly.express as px
import plotly.graph_objects as go
from utils.rich_content_oz import (
    ScoringConfig, RichContentProcessor, ProcessingResult, 
    BatchResult, ProcessingStatus
)


def render_scoring_config_ui() -> ScoringConfig:
    """
    Рендерит UI для конфигурации системы оценки схожести
    
    Returns:
        Настроенная конфигурация ScoringConfig
    """
    st.subheader("⚙️ Конфигурация алгоритма")
    
    # Выбор пресета
    preset_options = {
        "balanced": "🎯 Сбалансированный",
        "size_focused": "📏 Фокус на размере", 
        "seasonal": "🌦️ Сезонный",
        "material_focused": "🧵 Фокус на материале",
        "conservative": "🛡️ Консервативный"
    }
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        preset_key = st.selectbox(
            "Выберите пресет",
            options=list(preset_options.keys()),
            format_func=lambda x: preset_options[x],
            help="Предустановленные конфигурации для разных сценариев"
        )
        
        config = ScoringConfig.get_preset(preset_key)
        
        # Показываем описание пресета
        preset_descriptions = {
            "balanced": "Универсальная конфигурация с равным весом всех параметров",
            "size_focused": "Приоритет точному совпадению размера",
            "seasonal": "Усиленный фокус на сезонности товаров",
            "material_focused": "Приоритет материалам и колодкам",
            "conservative": "Строгие критерии отбора, меньше рекомендаций"
        }
        st.info(preset_descriptions[preset_key])
    
    with col2:
        st.write("**Основные параметры:**")
        st.metric("Базовый score", config.base_score)
        st.metric("Максимальный score", config.max_score)
        st.metric("Минимальный порог", f"{config.min_score_threshold:.1f}")
        st.metric("Мин. рекомендаций", config.min_recommendations)
        st.metric("Макс. рекомендаций", config.max_recommendations)
    
    # Расширенные настройки
    st.write("**🔧 Расширенные настройки:**")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.write("**Размеры:**")
        config.exact_size_weight = st.number_input(
            "Точное совпадение", 
            min_value=0, max_value=200, 
            value=config.exact_size_weight
        )
        config.close_size_weight = st.number_input(
            "Близкий размер (±1)", 
            min_value=0, max_value=100, 
            value=config.close_size_weight
        )
        config.size_mismatch_penalty = st.number_input(
            "Штраф за несовпадение", 
            min_value=-100, max_value=0, 
            value=config.size_mismatch_penalty
        )
    
    with col2:
        st.write("**Сезонность:**")
        config.season_match_bonus = st.number_input(
            "Бонус за сезон", 
            min_value=0, max_value=150, 
            value=config.season_match_bonus
        )
        config.season_mismatch_penalty = st.number_input(
            "Штраф за сезон", 
            min_value=-100, max_value=0, 
            value=config.season_mismatch_penalty
        )
    
    with col3:
        st.write("**Дополнительно:**")
        config.color_match_bonus = st.number_input(
            "Бонус за цвет", 
            min_value=0, max_value=100, 
            value=config.color_match_bonus
        )
        config.material_match_bonus = st.number_input(
            "Бонус за материал", 
            min_value=0, max_value=100, 
            value=config.material_match_bonus
        )
        config.fastener_match_bonus = st.number_input(
            "Бонус за застёжку", 
            min_value=0, max_value=100, 
            value=config.fastener_match_bonus
        )
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Колодки:**")
        config.mega_last_bonus = st.number_input(
            "Mega last", min_value=0, max_value=150, 
            value=config.mega_last_bonus
        )
        config.best_last_bonus = st.number_input(
            "Best last", min_value=0, max_value=120, 
            value=config.best_last_bonus
        )
        config.new_last_bonus = st.number_input(
            "New last", min_value=0, max_value=100, 
            value=config.new_last_bonus
        )
    
    with col2:
        st.write("**Лимиты:**")
        config.min_recommendations = st.number_input(
            "Мин. рекомендаций", 
            min_value=1, max_value=20, 
            value=config.min_recommendations,
            help="Минимальное количество рекомендаций для создания Rich Content"
        )
        config.max_recommendations = st.number_input(
            "Макс. рекомендаций", 
            min_value=config.min_recommendations, max_value=20, 
            value=config.max_recommendations
        )
        config.min_score_threshold = st.number_input(
            "Минимальный score", 
            min_value=0.0, max_value=200.0, 
            value=config.min_score_threshold, 
            step=10.0
        )
    
    return config


def render_product_selector(db_conn) -> Tuple[str, List[str]]:
    """
    Рендерит UI для выбора товаров для обработки
    
    Args:
        db_conn: Подключение к базе данных
        
    Returns:
        Tuple[selection_mode, selected_products] - режим выбора и список товаров
    """
    st.subheader("📦 Выбор товаров для обработки")
    
    # Статистика БД
    try:
        stats_query = """
        SELECT 
            COUNT(*) as total_products,
            COUNT(CASE WHEN rich_content_json IS NOT NULL AND rich_content_json != '' 
                  THEN 1 END) as with_rich_content,
            COUNT(CASE WHEN oz_fbo_stock > 0 THEN 1 END) as in_stock
        FROM oz_category_products ocp
        LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
        """
        stats = db_conn.execute(stats_query).fetchone()
        
        if stats:
            total, with_rich, in_stock = stats
            coverage = (with_rich / total * 100) if total > 0 else 0
            
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Всего товаров", total)
            col2.metric("С Rich Content", with_rich)
            col3.metric("В наличии", in_stock)
            col4.metric("Покрытие", f"{coverage:.1f}%")
    except Exception as e:
        st.warning(f"Не удалось получить статистику БД: {e}")
    
    # Режим выбора
    selection_mode = st.radio(
        "Режим выбора товаров:",
        ["single", "list", "filter", "all"],
        format_func=lambda x: {
            "single": "🎯 Один товар",
            "list": "📝 Список артикулов", 
            "filter": "🔍 По фильтрам",
            "all": "🌐 Все товары"
        }[x],
        horizontal=True
    )
    
    selected_products = []
    
    if selection_mode == "single":
        vendor_code = st.text_input(
            "Артикул товара",
            placeholder="Введите oz_vendor_code",
            help="Например: ABC-123-XL"
        )
        if vendor_code.strip():
            selected_products = [vendor_code.strip()]
    
    elif selection_mode == "list":
        vendor_codes_text = st.text_area(
            "Список артикулов",
            placeholder="Введите артикулы, каждый с новой строки",
            height=100,
            help="Один артикул на строку"
        )
        if vendor_codes_text.strip():
            selected_products = [
                code.strip() for code in vendor_codes_text.strip().split('\n') 
                if code.strip()
            ]
    
    elif selection_mode == "filter":
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Фильтр по брендам
            brands_query = "SELECT DISTINCT oz_brand FROM oz_category_products WHERE oz_brand IS NOT NULL ORDER BY oz_brand"
            try:
                brands_df = pd.read_sql(brands_query, db_conn)
                selected_brands = st.multiselect(
                    "Бренды",
                    options=brands_df['oz_brand'].tolist(),
                    help="Выберите бренды для фильтрации"
                )
            except:
                selected_brands = []
                st.warning("Не удалось загрузить список брендов")
        
        with col2:
            # Фильтр по типам
            types_query = "SELECT DISTINCT type FROM oz_category_products WHERE type IS NOT NULL ORDER BY type"
            try:
                types_df = pd.read_sql(types_query, db_conn)
                selected_types = st.multiselect(
                    "Типы товаров",
                    options=types_df['type'].tolist(),
                    help="Выберите типы товаров"
                )
            except:
                selected_types = []
                st.warning("Не удалось загрузить список типов")
        
        with col3:
            # Фильтр по полу
            genders = ["Женский", "Мужской", "Унисекс"]
            selected_genders = st.multiselect(
                "Пол",
                options=genders,
                help="Выберите целевой пол"
            )
        
        # Дополнительные фильтры
        col1, col2 = st.columns(2)
        
        with col1:
            only_in_stock = st.checkbox(
                "Только товары в наличии",
                value=True,
                help="Фильтровать товары с oz_fbo_stock > 0"
            )
            
            without_rich_content = st.checkbox(
                "Только без Rich Content",
                value=True,
                help="Обрабатывать только товары без существующего контента"
            )
        
        with col2:
            limit = st.number_input(
                "Лимит товаров",
                min_value=1,
                max_value=1000,
                value=50,
                help="Максимальное количество товаров для обработки"
            )
        
        # Формируем запрос
        if st.button("🔍 Найти товары по фильтрам"):
            conditions = ["1=1"]
            params = []
            
            if selected_brands:
                conditions.append(f"ocp.oz_brand IN ({','.join(['?' for _ in selected_brands])})")
                params.extend(selected_brands)
            
            if selected_types:
                conditions.append(f"ocp.type IN ({','.join(['?' for _ in selected_types])})")
                params.extend(selected_types)
            
            if selected_genders:
                conditions.append(f"ocp.gender IN ({','.join(['?' for _ in selected_genders])})")
                params.extend(selected_genders)
            
            if only_in_stock:
                conditions.append("(op.oz_fbo_stock > 0 OR op.oz_fbo_stock IS NULL)")
            
            if without_rich_content:
                conditions.append("(ocp.rich_content_json IS NULL OR ocp.rich_content_json = '')")
            
            query = f"""
            SELECT DISTINCT ocp.oz_vendor_code
            FROM oz_category_products ocp
            LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
            WHERE {' AND '.join(conditions)}
            ORDER BY ocp.oz_vendor_code
            LIMIT ?
            """
            params.append(limit)
            
            try:
                result_df = pd.read_sql(query, db_conn, params=params)
                selected_products = result_df['oz_vendor_code'].tolist()
                st.success(f"Найдено {len(selected_products)} товаров")
                
                if selected_products:
                    # Показываем превью
                    with st.expander("📋 Предпросмотр найденных товаров"):
                        preview_df = pd.DataFrame({'oz_vendor_code': selected_products[:10]})
                        st.dataframe(preview_df, use_container_width=True)
                        if len(selected_products) > 10:
                            st.info(f"... и еще {len(selected_products) - 10} товаров")
            except Exception as e:
                st.error(f"Ошибка выполнения фильтрации: {e}")
    
    elif selection_mode == "all":
        limit = st.number_input(
            "Лимит товаров (для безопасности)",
            min_value=1,
            max_value=5000,
            value=100,
            help="Максимальное количество товаров для обработки"
        )
        
        if st.button("⚠️ Загрузить все товары (осторожно!)"):
            try:
                query = """
                SELECT oz_vendor_code 
                FROM oz_category_products 
                WHERE oz_vendor_code IS NOT NULL 
                ORDER BY oz_vendor_code 
                LIMIT ?
                """
                result_df = pd.read_sql(query, db_conn, params=[limit])
                selected_products = result_df['oz_vendor_code'].tolist()
                st.success(f"Загружено {len(selected_products)} товаров")
            except Exception as e:
                st.error(f"Ошибка загрузки всех товаров: {e}")
    
    # Итоговая информация
    if selected_products:
        st.info(f"📊 Выбрано товаров для обработки: **{len(selected_products)}**")
    
    return selection_mode, selected_products


def render_rich_content_preview(rich_content_json: str) -> None:
    """
    Рендерит превью Rich Content JSON
    
    Args:
        rich_content_json: JSON строка Rich Content
    """
    st.subheader("👁️ Превью Rich Content")
    
    try:
        data = json.loads(rich_content_json)
        
        # Вкладки для разных видов отображения
        tab1, tab2, tab3 = st.tabs(["🎨 Визуальный превью", "📋 JSON структура", "🔍 Детали"])
        
        with tab1:
            # Визуализация контента
            if 'content' in data:
                for i, widget in enumerate(data['content']):
                    widget_name = widget.get('widgetName', 'Unknown')
                    
                    if widget_name == 'raText':
                        st.markdown(f"**{widget['text']['content']}**")
                        
                    elif widget_name == 'raShowcase':
                        st.write("**Карусель товаров:**")
                        if 'blocks' in widget:
                            # Показываем все товары, но в строках по 4 для лучшей визуализации
                            total_blocks = len(widget['blocks'])
                            for row_start in range(0, total_blocks, 4):
                                row_blocks = widget['blocks'][row_start:row_start + 4]
                                cols = st.columns(len(row_blocks))
                                for j, block in enumerate(row_blocks):
                                    with cols[j]:
                                        if 'img' in block:
                                            st.image(
                                                block['img'].get('src', ''), 
                                                caption=block.get('title', {}).get('content', 'Товар'),
                                                width=150
                                            )
                                        if 'subtitle' in block:
                                            st.caption(block['subtitle']['content'])
                    
                    elif widget_name == 'raColumns':
                        st.write("**Сетка товаров:**")
                        if 'columns' in widget:
                            cols = st.columns(len(widget['columns']))
                            for j, column in enumerate(widget['columns']):
                                with cols[j]:
                                    if 'img' in column:
                                        st.image(
                                            column['img'].get('src', ''),
                                            width=100
                                        )
                                    if 'content' in column:
                                        st.caption(column['content'].get('title', ''))
            else:
                st.warning("Контент не найден в JSON")
        
        with tab2:
            # JSON со подсветкой синтаксиса
            st.json(data)
        
        with tab3:
            # Детальная информация
            version = data.get('version', 'Не указана')
            content_count = len(data.get('content', []))
            
            col1, col2 = st.columns(2)
            col1.metric("Версия Rich Content", version)
            col2.metric("Количество виджетов", content_count)
            
            # Анализ виджетов
            if 'content' in data:
                widget_types = [w.get('widgetName', 'Unknown') for w in data['content']]
                widget_counts = pd.Series(widget_types).value_counts()
                
                st.write("**Используемые виджеты:**")
                for widget_type, count in widget_counts.items():
                    st.write(f"- {widget_type}: {count}")
    
    except json.JSONDecodeError as e:
        st.error(f"Ошибка парсинга JSON: {e}")
        st.code(rich_content_json)
    except Exception as e:
        st.error(f"Ошибка отображения превью: {e}")


def render_processing_results(processing_result: ProcessingResult, use_expanders: bool = True) -> None:
    """
    Рендерит результат обработки одного товара
    
    Args:
        processing_result: Результат обработки товара
        use_expanders: Использовать ли expander'ы (False для избежания вложенности)
    """
    vendor_code = processing_result.oz_vendor_code
    status = processing_result.status
    
    # Статус с цветом
    status_colors = {
        ProcessingStatus.SUCCESS: "🟢",
        ProcessingStatus.NO_SIMILAR: "🟡", 
        ProcessingStatus.ERROR: "🔴",
        ProcessingStatus.NO_DATA: "⚪"
    }
    
    status_color = status_colors.get(status, "⚫")
    st.write(f"**{status_color} {vendor_code}** - {status.value}")
    
    # Детали результата
    col1, col2, col3 = st.columns(3)
    col1.metric("Рекомендаций", len(processing_result.recommendations))
    col2.metric("Время обработки", f"{processing_result.processing_time*1000:.1f}ms")
    
    if status == ProcessingStatus.SUCCESS:
        col3.metric("Статус", "✅ Успешно")
        
        # Показываем рекомендации
        if processing_result.recommendations:
            if use_expanders:
                with st.expander(f"📋 Рекомендации ({len(processing_result.recommendations)})"):
                    for i, rec in enumerate(processing_result.recommendations):
                        st.write(f"**{i+1}.** {rec.product_info.oz_vendor_code} "
                                f"(Score: {rec.score:.1f}) - {rec.match_details}")
            else:
                st.write(f"**📋 Рекомендации ({len(processing_result.recommendations)}):**")
                for i, rec in enumerate(processing_result.recommendations):
                    st.write(f"**{i+1}.** {rec.product_info.oz_vendor_code} "
                            f"(Score: {rec.score:.1f}) - {rec.match_details}")
        
        # Превью Rich Content
        if processing_result.rich_content_json:
            if use_expanders:
                with st.expander("👁️ Rich Content"):
                    render_rich_content_preview(processing_result.rich_content_json)
            else:
                st.write("**👁️ Rich Content:**")
                render_rich_content_preview(processing_result.rich_content_json)
    
    elif status == ProcessingStatus.ERROR:
        col3.metric("Статус", "❌ Ошибка")
        if processing_result.error_message:
            st.error(f"Ошибка: {processing_result.error_message}")
    
    elif status == ProcessingStatus.NO_SIMILAR:
        col3.metric("Статус", "⚠️ Нет похожих")
        st.warning("Не найдено похожих товаров для рекомендаций")
    
    else:
        col3.metric("Статус", "❓ Другое")


def render_batch_results(batch_result: BatchResult) -> None:
    """
    Рендерит результаты пакетной обработки
    
    Args:
        batch_result: Результат пакетной обработки
    """
    st.subheader("📊 Результаты пакетной обработки")
    
    # Общая статистика
    stats = batch_result.stats
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Всего обработано", batch_result.total_items)
    col2.metric("Успешно", stats['successful'])
    col3.metric("Без похожих", stats['no_similar'])
    col4.metric("Ошибок", stats['errors'])
    
    # Успешность обработки
    success_rate = stats['success_rate']
    st.metric("Процент успеха", f"{success_rate}%")
    
    # Диаграмма статусов
    if batch_result.processed_items:
        status_counts = {}
        for item in batch_result.processed_items:
            status = item.status.value
            status_counts[status] = status_counts.get(status, 0) + 1
        
        # Pie chart статусов
        fig = px.pie(
            values=list(status_counts.values()),
            names=list(status_counts.keys()),
            title="Распределение статусов обработки",
            color_discrete_map={
                'success': '#28a745',
                'no_similar': '#ffc107', 
                'error': '#dc3545',
                'no_data': '#6c757d'
            }
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Детальные результаты
        with st.expander("📋 Детальные результаты"):
            # Группировка по статусам
            tab1, tab2, tab3 = st.tabs(["✅ Успешные", "⚠️ Без похожих", "❌ Ошибки"])
            
            with tab1:
                successful_items = [item for item in batch_result.processed_items 
                                  if item.status == ProcessingStatus.SUCCESS]
                if successful_items:
                    for item in successful_items:
                        render_processing_results(item, use_expanders=False)
                        st.divider()
                else:
                    st.info("Нет успешно обработанных товаров")
            
            with tab2:
                no_similar_items = [item for item in batch_result.processed_items 
                                   if item.status == ProcessingStatus.NO_SIMILAR]
                if no_similar_items:
                    for item in no_similar_items:
                        st.write(f"🟡 {item.oz_vendor_code}")
                else:
                    st.info("Нет товаров без похожих")
            
            with tab3:
                error_items = [item for item in batch_result.processed_items 
                              if item.status == ProcessingStatus.ERROR]
                if error_items:
                    for item in error_items:
                        st.write(f"🔴 {item.oz_vendor_code}: {item.error_message}")
                else:
                    st.info("Нет ошибок")


def render_processing_statistics(processor: RichContentProcessor) -> None:
    """
    Рендерит статистику обработки из базы данных
    
    Args:
        processor: Экземпляр RichContentProcessor
    """
    st.subheader("📈 Статистика базы данных")
    
    try:
        stats = processor.get_processing_statistics()
        
        if 'error' in stats:
            st.error(f"Ошибка получения статистики: {stats['error']}")
            return
        
        # Основные метрики
        col1, col2, col3, col4 = st.columns(4)
        
        col1.metric("Всего товаров", stats['total_products'])
        col2.metric("С Rich Content", stats['products_with_rich_content'])
        col3.metric("Без Rich Content", stats['products_without_rich_content'])
        col4.metric("Покрытие", f"{stats['coverage_percent']:.1f}%")
        
        # Прогресс бар покрытия
        coverage = stats['coverage_percent']
        st.progress(coverage / 100, text=f"Покрытие Rich Content: {coverage:.1f}%")
        
        # Визуализация
        if stats['total_products'] > 0:
            fig = go.Figure(data=[
                go.Bar(
                    x=['С Rich Content', 'Без Rich Content'],
                    y=[stats['products_with_rich_content'], stats['products_without_rich_content']],
                    marker_color=['#28a745', '#dc3545']
                )
            ])
            fig.update_layout(
                title="Статистика Rich Content в базе данных",
                yaxis_title="Количество товаров"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    except Exception as e:
        st.error(f"Ошибка отображения статистики: {e}") 