"""
CSV Exporter для Rich Content - Экспорт результатов в CSV формат.

Основные функции:
- Экспорт результатов обработки в CSV
- Поддержка больших объемов данных
- Интеграция с Streamlit download_button
- Валидация данных перед экспортом
"""

import csv
import io
import json
import logging
from typing import List, Dict, Any, Optional
import streamlit as st
import pandas as pd

logger = logging.getLogger(__name__)

class RichContentCSVExporter:
    """
    Класс для экспорта результатов Rich Content в CSV формат.
    
    Формат CSV:
    - oz_vendor_code: артикул товара
    - rich_content_json: JSON строка с Rich Content
    """
    
    def __init__(self):
        """Инициализация экспортера"""
        logger.debug("RichContentCSVExporter инициализирован")
    
    def export_to_csv_string(self, results: List[Dict[str, Any]]) -> str:
        """
        Экспорт результатов в CSV строку.
        
        Args:
            results: Список результатов обработки
            
        Returns:
            CSV строка
        """
        if not results:
            logger.warning("Нет данных для экспорта")
            return ""
        
        # Фильтруем только успешные результаты с Rich Content
        valid_results = [
            r for r in results 
            if r.get('success') and r.get('rich_content_json')
        ]
        
        if not valid_results:
            logger.warning("Нет валидных результатов для экспорта")
            return ""
        
        logger.info(f"Экспортируем {len(valid_results)} записей в CSV")
        
        # Создаем CSV в памяти
        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
        
        # Заголовки - добавляем третий столбец для детализации
        writer.writerow(['oz_vendor_code', 'rich_content_json', 'scoring_details'])
        
        # Данные
        for result in valid_results:
            vendor_code = result.get('oz_vendor_code', '')
            rich_content = result.get('rich_content_json', '')
            scoring_details = self._generate_scoring_details(result)
            
            # Валидация JSON
            if rich_content and self._validate_json(rich_content):
                writer.writerow([vendor_code, rich_content, scoring_details])
            else:
                logger.warning(f"Пропускаем {vendor_code}: невалидный JSON")
        
        csv_content = output.getvalue()
        output.close()
        
        logger.info(f"CSV экспорт завершен: {len(csv_content)} символов")
        return csv_content
    
    def export_to_dataframe(self, results: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Экспорт результатов в pandas DataFrame.
        
        Args:
            results: Список результатов обработки
            
        Returns:
            DataFrame с результатами
        """
        if not results:
            return pd.DataFrame(columns=['oz_vendor_code', 'rich_content_json'])
        
        # Фильтруем только успешные результаты
        valid_results = [
            {
                'oz_vendor_code': r.get('oz_vendor_code', ''),
                'rich_content_json': r.get('rich_content_json', '')
            }
            for r in results 
            if r.get('success') and r.get('rich_content_json')
        ]
        
        df = pd.DataFrame(valid_results)
        logger.info(f"Создан DataFrame с {len(df)} записями")
        
        return df
    
    def create_download_button(
        self, 
        results: List[Dict[str, Any]], 
        filename: Optional[str] = None,
        button_label: str = "📥 Скачать CSV"
    ) -> bool:
        """
        Создает кнопку скачивания CSV в Streamlit.
        
        Args:
            results: Список результатов обработки
            filename: Имя файла для скачивания
            button_label: Текст на кнопке
            
        Returns:
            True если кнопка была нажата
        """
        if not results:
            st.warning("Нет данных для экспорта")
            return False
        
        # Генерируем CSV
        csv_content = self.export_to_csv_string(results)
        
        if not csv_content:
            st.warning("Нет валидных данных для экспорта")
            return False
        
        # Имя файла по умолчанию
        if not filename:
            import time
            filename = f"rich_content_export_{int(time.time())}.csv"
        
        # Создаем кнопку скачивания
        clicked = st.download_button(
            label=button_label,
            data=csv_content,
            file_name=filename,
            mime='text/csv',
            use_container_width=True,
            type="primary"
        )
        
        if clicked:
            logger.info(f"CSV файл скачан: {filename}")
        
        return clicked
    
    def get_export_preview(
        self, 
        results: List[Dict[str, Any]], 
        max_rows: int = 10
    ) -> pd.DataFrame:
        """
        Получение превью данных для экспорта.
        
        Args:
            results: Список результатов обработки
            max_rows: Максимальное количество строк для превью
            
        Returns:
            DataFrame с превью данных
        """
        if not results:
            return pd.DataFrame()
        
        # Создаем превью с дополнительной информацией
        preview_data = []
        
        for result in results[:max_rows]:
            vendor_code = result.get('oz_vendor_code', '')
            rich_content = result.get('rich_content_json', '')
            success = result.get('success', False)
            recommendations_count = result.get('recommendations_count', 0)
            processing_time = result.get('processing_time', 0)
            
            # Краткая информация о JSON
            json_info = "❌ Нет данных"
            if rich_content:
                try:
                    json_data = json.loads(rich_content)
                    json_info = f"✅ {len(json_data.get('items', []))} товаров"
                except:
                    json_info = "⚠️ Невалидный JSON"
            
            preview_data.append({
                'oz_vendor_code': vendor_code,
                'success': "✅" if success else "❌",
                'recommendations': recommendations_count,
                'processing_time_ms': round(processing_time * 1000, 1),
                'rich_content_status': json_info
            })
        
        df = pd.DataFrame(preview_data)
        logger.debug(f"Создано превью с {len(df)} записями")
        
        return df
    
    def _validate_json(self, json_string: str) -> bool:
        """
        Валидация JSON строки.
        
        Args:
            json_string: JSON строка для валидации
            
        Returns:
            True если JSON валидный
        """
        if not json_string or not isinstance(json_string, str):
            return False
        
        try:
            json.loads(json_string)
            return True
        except (json.JSONDecodeError, TypeError):
            return False
    
    def get_export_statistics(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Получение статистики экспорта.
        
        Args:
            results: Список результатов обработки
            
        Returns:
            Словарь со статистикой
        """
        if not results:
            return {
                'total_results': 0,
                'exportable_results': 0,
                'export_rate': 0.0,
                'total_size_bytes': 0
            }
        
        exportable = [
            r for r in results 
            if r.get('success') and r.get('rich_content_json')
        ]
        
        # Размер CSV
        csv_content = self.export_to_csv_string(results)
        size_bytes = len(csv_content.encode('utf-8'))
        
        stats = {
            'total_results': len(results),
            'exportable_results': len(exportable),
            'export_rate': len(exportable) / len(results) * 100 if results else 0,
            'total_size_bytes': size_bytes,
            'size_mb': round(size_bytes / 1024 / 1024, 2)
        }
        
        logger.info(f"Статистика экспорта: {stats}")
        return stats
    
    def _generate_scoring_details(self, result: Dict[str, Any]) -> str:
        """
        Генерация детализированной информации о scoring для CSV.
        
        Args:
            result: Результат обработки одного товара
            
        Returns:
            Строка с детализацией scoring
        """
        try:
            # Используем готовую детализацию из результата обработки
            scoring_details = result.get('scoring_details', '')
            if scoring_details:
                return scoring_details
            
            # Fallback: генерируем базовую информацию
            details = []
            
            recommendations_count = result.get('recommendations_count', 0)
            details.append(f"Рекомендаций: {recommendations_count}")
            
            processing_time = result.get('processing_time', 0)
            details.append(f"Время: {processing_time:.2f}с")
            
            if result.get('success'):
                details.append("Статус: успешно")
            else:
                error_msg = result.get('error_message', 'неизвестная ошибка')
                details.append(f"Ошибка: {error_msg}")
            
            return " | ".join(details)
            
        except Exception as e:
            logger.warning(f"Ошибка генерации scoring details: {e}")
            return f"Ошибка анализа: {str(e)}"
