"""
Shared UI components for WB Recommendations system.

This module contains reusable UI components and utilities to reduce code duplication
and improve maintainability in the WB recommendations interface.

Author: DataFox SL Project - Optimized Version
Version: 2.0.0
"""

import streamlit as st
import pandas as pd
import time
import io
import logging
from typing import List, Dict, Any, Optional, Callable, Tuple
from functools import wraps

# Import WB-specific classes
from .wb_recommendations import WBScoringConfig, WBBatchResult, WBProcessingStatus
from .manual_recommendations_manager import ManualRecommendationsManager

# Smart logging configuration
logger = logging.getLogger(__name__)

class UILogger:
    """Smart logging system with reduced overhead"""
    
    @staticmethod
    def log_ui_action(action: str, details: str = ""):
        """Log UI actions with minimal overhead"""
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"🎛️ {action}: {details}")
    
    @staticmethod
    def log_error(component: str, error: Exception):
        """Log errors with consistent format"""
        logger.error(f"❌ {component} error: {error}")

def handle_ui_errors(component_name: str):
    """Decorator for consistent error handling in UI components"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                UILogger.log_error(component_name, e)
                st.error(f"❌ Ошибка в компоненте {component_name}: {e}")
                return None
        return wrapper
    return decorator

@st.cache_data(ttl=300)
def get_preset_options():
    """Cached preset options for scoring config"""
    return {
        "balanced": "⚖️ Сбалансированный",
        "size_focused": "📏 Фокус на размере", 
        "price_focused": "💰 Фокус на цене",
        "quality_focused": "⭐ Фокус на качестве",
        "conservative": "🔒 Консервативный"
    }

class SessionManager:
    """Centralized session state management"""
    
    @staticmethod
    def initialize_wb_session():
        """Initialize all WB-related session state variables"""
        defaults = {
            'wb_recommendation_processor': None,
            'wb_batch_result': None,
            'wb_skus_input': "",
            'manual_recommendations_manager': None
        }
        
        for key, default_value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = default_value
        
        UILogger.log_ui_action("Session initialized", f"Keys: {list(defaults.keys())}")
    
    @staticmethod
    def clear_results():
        """Clear processing results from session"""
        st.session_state.wb_batch_result = None
        UILogger.log_ui_action("Results cleared")

class ProgressTracker:
    """Reusable progress tracking component"""
    
    def __init__(self):
        self.progress_bar = st.progress(0)
        self.status_text = st.empty()
    
    def update(self, current: int, total: int, message: str):
        """Update progress with consistent formatting"""
        progress = current / total if total > 0 else 0
        self.progress_bar.progress(progress)
        self.status_text.text(f"Обработано {current}/{total}: {message}")

@handle_ui_errors("scoring_config")
def render_scoring_config() -> Optional[WBScoringConfig]:
    """Compact scoring configuration UI"""
    st.subheader("⚙️ Конфигурация алгоритма")
    
    # Cached preset selection
    preset_options = get_preset_options()
    preset = st.selectbox(
        "Выберите пресет:",
        list(preset_options.keys()),
        format_func=lambda x: preset_options[x],
        help="Предустановленная конфигурация алгоритма"
    )
    
    config = WBScoringConfig.get_preset(preset)
    
    # Compact advanced settings
    with st.expander("🔧 Дополнительные настройки"):
        col1, col2 = st.columns(2)
        
        with col1:
            config.max_recommendations = st.slider(
                "Макс. рекомендаций:", 5, 50, config.max_recommendations
            )
            config.min_recommendations = st.slider(
                "Мин. рекомендаций:", 1, 25, config.min_recommendations
            )
        
        with col2:
            config.min_score_threshold = st.slider(
                "Мин. порог score:", 0.0, 200.0, config.min_score_threshold
            )
            config.exact_size_weight = st.slider(
                "Точный размер:", 0, 200, config.exact_size_weight
            )
    
    UILogger.log_ui_action("Config rendered", f"preset={preset}")
    return config

@handle_ui_errors("wb_skus_input")
def render_wb_skus_input() -> List[str]:
    """Streamlined WB SKU input component"""
    st.subheader("📝 Ввод WB SKU")
    
    input_method = st.radio(
        "Способ ввода:",
        ["text", "file"],
        format_func=lambda x: "📝 Текст" if x == "text" else "📄 Файл",
        horizontal=True
    )
    
    wb_skus = []
    
    if input_method == "text":
        input_text = st.text_area(
            "WB SKU (по одному на строку или через запятую):",
            value=st.session_state.wb_skus_input,
            height=150,
            placeholder="123456789\n987654321"
        )
        st.session_state.wb_skus_input = input_text
        wb_skus = _parse_wb_skus(input_text)
    else:
        wb_skus = _handle_file_upload()
    
    if wb_skus:
        _display_skus_preview(wb_skus)
    
    return wb_skus

def _parse_wb_skus(input_text: str) -> List[str]:
    """Parse WB SKUs from text input"""
    if not input_text.strip():
        return []
    
    import re
    skus = re.split(r'[\n,;\s]+', input_text.strip())
    cleaned_skus = [sku.strip() for sku in skus if sku.strip().isdigit()]
    
    UILogger.log_ui_action("SKUs parsed", f"Found {len(cleaned_skus)} valid SKUs")
    return cleaned_skus

def _handle_file_upload() -> List[str]:
    """Handle file upload for WB SKUs"""
    uploaded_file = st.file_uploader(
        "Загрузите файл с WB SKU:",
        type=['txt', 'csv', 'xlsx'],
        help="Поддерживаются: TXT, CSV, XLSX"
    )
    
    if not uploaded_file:
        return []
    
    try:
        if uploaded_file.type == "text/plain":
            content = uploaded_file.read().decode('utf-8')
            return _parse_wb_skus(content)
        elif uploaded_file.type == "text/csv":
            df = pd.read_csv(uploaded_file)
            return df.iloc[:, 0].astype(str).str.strip().tolist()
        elif "spreadsheet" in uploaded_file.type:
            df = pd.read_excel(uploaded_file)
            return df.iloc[:, 0].astype(str).str.strip().tolist()
    except Exception as e:
        st.error(f"❌ Ошибка загрузки файла: {e}")
        return []
    
    return []

def _display_skus_preview(wb_skus: List[str]):
    """Display preview of loaded SKUs"""
    st.info(f"📊 Найдено {len(wb_skus)} валидных WB SKU")
    
    if len(wb_skus) <= 10:
        with st.expander("📋 Все SKU"):
            for i, sku in enumerate(wb_skus, 1):
                st.write(f"{i}. {sku}")
    else:
        with st.expander("📋 Превью первых 10 SKU"):
            for i, sku in enumerate(wb_skus[:10], 1):
                st.write(f"{i}. {sku}")
            st.write(f"... и еще {len(wb_skus) - 10} SKU")

@handle_ui_errors("batch_results") 
def render_batch_results_compact(batch_result: WBBatchResult):
    """Compact batch results display"""
    st.subheader("📊 Результаты обработки")
    
    # Metrics in a more compact layout
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Обработано", len(batch_result.processed_items))
    with col2:
        st.metric("Успешно", batch_result.success_count)  
    with col3:
        st.metric("Ошибок", batch_result.error_count)
    with col4:
        st.metric("Успешность", f"{batch_result.success_rate:.1f}%")
    
    st.info(f"⏱️ Время: {batch_result.total_processing_time:.1f} сек")
    
    # Compact results table with filtering
    _render_results_table(batch_result)

def _render_results_table(batch_result: WBBatchResult):
    """Render results table with filtering"""
    results_data = []
    for result in batch_result.processed_items:
        results_data.append({
            "WB SKU": result.wb_sku,
            "Статус": result.status.value,
            "Рекомендации": len(result.recommendations),
            "Время (с)": f"{result.processing_time:.2f}",
            "Ошибка": result.error_message or ""
        })
    
    results_df = pd.DataFrame(results_data)
    
    # Status filter
    status_options = ["Все"] + [s.value for s in WBProcessingStatus]
    status_filter = st.selectbox(
        "Фильтр по статусу:", 
        status_options,
        format_func=lambda x: _get_status_emoji(x) + " " + x.replace("_", " ").title()
    )
    
    if status_filter != "Все":
        results_df = results_df[results_df["Статус"] == status_filter]
    
    st.dataframe(results_df, use_container_width=True, height=300)
    
    # Export button
    if st.button("📥 Экспорт результатов"):
        csv = results_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Скачать CSV",
            data=csv,
            file_name=f"wb_results_{int(time.time())}.csv",
            mime="text/csv"
        )

def _get_status_emoji(status: str) -> str:
    """Get emoji for status"""
    emoji_map = {
        "success": "✅",
        "no_similar": "⚠️", 
        "insufficient_recommendations": "📉",
        "error": "❌",
        "no_data": "📭",
        "no_ozon_link": "🔗"
    }
    return emoji_map.get(status, "🔍")

@handle_ui_errors("manual_recommendations")
def render_manual_recommendations_compact() -> Optional[ManualRecommendationsManager]:
    """Compact manual recommendations UI"""
    st.subheader("🖐️ Ручные рекомендации")
    
    with st.expander("📎 Загрузка файла", expanded=False):
        _show_format_help()
        
        # File upload
        manual_file = st.file_uploader(
            "Выберите файл:",
            type=['csv', 'xlsx'],
            help="CSV или Excel с ручными рекомендациями"
        )
        
        # Action buttons in compact layout
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("📄 Пример CSV", use_container_width=True):
                _download_example("csv")
        with col2:
            if st.button("📊 Пример Excel", use_container_width=True):
                _download_example("excel")  
        with col3:
            if st.button("🧹 Очистить", use_container_width=True):
                st.session_state.manual_recommendations_manager = None
                st.rerun()
        
        # Process uploaded file
        if manual_file:
            _process_manual_file(manual_file)
    
    # Show current stats
    _show_manual_stats()
    
    return st.session_state.manual_recommendations_manager

def _show_format_help():
    """Show format help in compact form"""
    st.markdown("""
    **Формат:** `target_wb_sku,position_1,recommended_sku_1,position_2,recommended_sku_2,...`
    
    **Пример:** `123123,2,321321,5,321456`
    """)

def _download_example(file_type: str):
    """Handle example file downloads"""
    manager = ManualRecommendationsManager()
    
    if file_type == "csv":
        data = manager.generate_example_csv()
        st.download_button(
            "📥 Скачать CSV",
            data=data,
            file_name="manual_recommendations_example.csv",
            mime="text/csv"
        )
    else:
        data = manager.generate_example_excel()
        st.download_button(
            "📥 Скачать Excel", 
            data=data,
            file_name="manual_recommendations_example.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

def _process_manual_file(manual_file):
    """Process uploaded manual recommendations file"""
    try:
        manager = ManualRecommendationsManager()
        file_ext = manual_file.name.lower().split('.')[-1]
        
        if file_ext == 'csv':
            success = manager.load_from_csv_file(manual_file)
        elif file_ext == 'xlsx':
            success = manager.load_from_excel_file(manual_file)
        else:
            st.error(f"❌ Неподдерживаемый формат: {file_ext}")
            return
        
        if success:
            st.session_state.manual_recommendations_manager = manager
            stats = manager.get_statistics()
            st.success(f"✅ Загружено: {stats['total_targets']} товаров, {stats['total_recommendations']} рекомендаций")
            UILogger.log_ui_action("Manual recommendations loaded", f"File: {manual_file.name}")
        else:
            st.error("❌ Ошибка загрузки файла")
            
    except Exception as e:
        st.error(f"❌ Критическая ошибка: {e}")
        UILogger.log_error("Manual file processing", e)

def _show_manual_stats():
    """Show current manual recommendations stats"""
    if st.session_state.manual_recommendations_manager:
        stats = st.session_state.manual_recommendations_manager.get_statistics()
        st.info(f"📊 **Активно:** {stats['total_targets']} товаров, {stats['total_recommendations']} рекомендаций")
    else:
        st.info("📋 Ручные рекомендации не загружены")

def create_export_table(batch_result: WBBatchResult) -> pd.DataFrame:
    """Create optimized export table"""
    table_data = []
    
    for result in batch_result.processed_items:
        if result.success and result.recommendations:
            for i, rec in enumerate(result.recommendations, 1):
                rec_num = rec.manual_position if rec.is_manual else i
                
                table_data.append({
                    "Исходный WB SKU": result.wb_sku,
                    "Рекомендация №": rec_num,
                    "Рекомендуемый WB SKU": rec.product_info.wb_sku,
                    "Тип": "🖐️ Ручная" if rec.is_manual else "🤖 Алгоритм",
                    "Score": round(rec.score, 1),
                    "Бренд": rec.product_info.wb_brand,
                    "Категория": rec.product_info.wb_category,
                    "Размеры": rec.product_info.get_size_range_str(),
                    "Остаток": rec.product_info.wb_fbo_stock,
                    "Цена": rec.product_info.wb_full_price,
                    "Обогащение": f"{rec.product_info.get_enrichment_score():.1%}",
                    "Источник": rec.product_info.enrichment_source
                })
    
    return pd.DataFrame(table_data)

def render_export_section(batch_result: WBBatchResult):
    """Render compact export section"""
    st.subheader("📊 Экспорт рекомендаций")
    
    df = create_export_table(batch_result)
    
    if df.empty:
        st.warning("⚠️ Нет данных для экспорта")
        return
    
    st.info(f"📊 Готово к экспорту: {len(df)} рекомендаций")
    st.dataframe(df.head(20), use_container_width=True)
    
    if len(df) > 20:
        st.info(f"Показаны первые 20 из {len(df)} строк")
    
    # Export buttons
    col1, col2 = st.columns(2)
    
    with col1:
        csv_data = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "📥 Скачать CSV",
            data=csv_data,
            file_name=f"wb_recommendations_{int(time.time())}.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    with col2:
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Рекомендации')
        
        st.download_button(
            "📥 Скачать Excel",
            data=excel_buffer.getvalue(),
            file_name=f"wb_recommendations_{int(time.time())}.xlsx", 
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

# Utility functions for better organization
class ComponentHelpers:
    """Helper functions for UI components"""
    
    @staticmethod
    def format_file_size(size_bytes: int) -> str:
        """Format file size in human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} TB"
    
    @staticmethod
    def truncate_text(text: str, max_length: int = 50) -> str:
        """Truncate text with ellipsis"""
        if len(text) <= max_length:
            return text
        return text[:max_length-3] + "..."