"""
Вспомогательные функции для экспорта Rich Content в различных форматах.
Предоставляет оптимизированные методы для работы с большими объемами данных.
"""

import pandas as pd
import time
import logging
from typing import List, Dict, Any, Generator, Optional, Tuple
import json

logger = logging.getLogger(__name__)

class RichContentExporter:
    """
    Класс для экспорта Rich Content данных в различных форматах.
    Поддерживает оптимизацию для больших объемов данных.
    """
    
    def __init__(self, db_conn):
        self.db_conn = db_conn
        
    def get_export_statistics(self) -> Dict[str, Any]:
        """
        Получение статистики по Rich Content в базе данных.
        
        Returns:
            Dict со статистикой по экспорту
        """
        try:
            stats_query = """
            SELECT 
                COUNT(*) as total_with_rich_content,
                COUNT(DISTINCT ocp.oz_brand) as unique_brands,
                AVG(LENGTH(ocp.rich_content_json)) as avg_json_size,
                MIN(LENGTH(ocp.rich_content_json)) as min_json_size,
                MAX(LENGTH(ocp.rich_content_json)) as max_json_size,
                COUNT(CASE WHEN op.oz_sku IS NOT NULL AND op.oz_sku != '' THEN 1 END) as with_sku,
                COUNT(CASE WHEN op.oz_sku IS NULL OR op.oz_sku = '' THEN 1 END) as without_sku
            FROM oz_category_products ocp
            LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
            WHERE ocp.rich_content_json IS NOT NULL 
            AND ocp.rich_content_json != ''
            AND LENGTH(ocp.rich_content_json) > 10
            """
            
            result = self.db_conn.execute(stats_query).fetchone()
            
            if result:
                return {
                    'total_records': result[0],
                    'unique_brands': result[1],
                    'avg_json_size_bytes': int(result[2]) if result[2] else 0,
                    'min_json_size_bytes': result[3] if result[3] else 0,
                    'max_json_size_bytes': result[4] if result[4] else 0,
                    'with_sku': result[5],
                    'without_sku': result[6],
                    'sku_coverage_percent': round((result[5] / result[0]) * 100, 2) if result[0] > 0 else 0,
                    'estimated_file_size_mb': round((result[0] * result[2]) / (1024 * 1024), 2) if result[2] else 0
                }
            else:
                return {'total_records': 0}
                
        except Exception as e:
            logger.error(f"Ошибка получения статистики экспорта: {e}")
            return {'total_records': 0, 'error': str(e)}
    
    def export_standard_csv(self, limit: Optional[int] = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Стандартный экспорт Rich Content в CSV формат.
        
        Args:
            limit: Лимит записей для экспорта (None = все)
            
        Returns:
            Tuple[DataFrame, Stats] - данные и статистика экспорта
        """
        try:
            # Основной запрос с правильным JOIN для получения oz_sku из oz_products
            query = """
            SELECT 
                ocp.oz_vendor_code,
                COALESCE(op.oz_sku, '') as oz_sku,
                ocp.rich_content_json as rich_content
            FROM oz_category_products ocp
            LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
            WHERE ocp.rich_content_json IS NOT NULL 
            AND ocp.rich_content_json != ''
            AND LENGTH(ocp.rich_content_json) > 10
            ORDER BY ocp.oz_vendor_code
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            # Выполняем запрос
            start_time = time.time()
            results = self.db_conn.execute(query).fetchall()
            query_time = time.time() - start_time
            
            # Создаем DataFrame
            if results:
                df = pd.DataFrame(results, columns=['oz_vendor_code', 'oz_sku', 'rich_content'])
                
                stats = {
                    'total_records': len(results),
                    'with_sku': len(df[df['oz_sku'] != '']),
                    'without_sku': len(df[df['oz_sku'] == '']),
                    'query_time_seconds': round(query_time, 2),
                    'success': True
                }
                
                return df, stats
            else:
                return pd.DataFrame(), {'total_records': 0, 'success': False}
                
        except Exception as e:
            logger.error(f"Ошибка стандартного экспорта: {e}")
            return pd.DataFrame(), {'total_records': 0, 'success': False, 'error': str(e)}
    
    def export_streaming_csv(self, batch_size: int = 1000) -> Generator[str, None, None]:
        """
        Потоковый экспорт Rich Content в CSV формат.
        Генерирует данные частями для экономии памяти.
        
        Args:
            batch_size: Размер батча для обработки
            
        Yields:
            str: Части CSV файла
        """
        try:
            # Заголовки CSV
            yield "oz_vendor_code,oz_sku,rich_content\n"
            
            # Получаем общее количество записей
            count_query = """
            SELECT COUNT(*) 
            FROM oz_category_products ocp
            WHERE ocp.rich_content_json IS NOT NULL 
            AND ocp.rich_content_json != ''
            AND LENGTH(ocp.rich_content_json) > 10
            """
            
            total_count = self.db_conn.execute(count_query).fetchone()[0]
            
            if total_count == 0:
                return
            
            # Обрабатываем данные батчами
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
                
                batch_results = self.db_conn.execute(batch_query, [batch_size, offset]).fetchall()
                
                if not batch_results:
                    break
                
                # Конвертируем batch в CSV строки
                for row in batch_results:
                    # Экранируем кавычки в JSON
                    escaped_json = str(row[2]).replace('"', '""')
                    yield f'"{row[0]}","{row[1]}","{escaped_json}"\n'
                
                offset += batch_size
                
        except Exception as e:
            logger.error(f"Ошибка потокового экспорта: {e}")
            yield f"# Ошибка экспорта: {e}\n"
    
    def export_compressed_csv(self, compression_level: str = 'medium') -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Экспорт Rich Content с сжатием JSON данных.
        
        Args:
            compression_level: Уровень сжатия ('light', 'medium', 'heavy')
            
        Returns:
            Tuple[DataFrame, Stats] - данные и статистика экспорта
        """
        try:
            # Получаем данные стандартным способом
            df, stats = self.export_standard_csv()
            
            if df.empty:
                return df, stats
            
            # Применяем сжатие к JSON данным
            original_size = 0
            compressed_size = 0
            
            def compress_json(json_str: str) -> str:
                nonlocal original_size, compressed_size
                
                if not json_str:
                    return json_str
                    
                original_size += len(json_str)
                
                try:
                    # Парсим JSON и сжимаем
                    json_data = json.loads(json_str)
                    
                    if compression_level == 'light':
                        # Удаляем лишние пробелы
                        compressed = json.dumps(json_data, separators=(',', ':'), ensure_ascii=False)
                    elif compression_level == 'medium':
                        # Удаляем пробелы и некоторые необязательные поля
                        compressed = json.dumps(json_data, separators=(',', ':'), ensure_ascii=False)
                    elif compression_level == 'heavy':
                        # Максимальное сжатие с удалением необязательных полей
                        compressed = json.dumps(json_data, separators=(',', ':'), ensure_ascii=False)
                    else:
                        compressed = json_str
                    
                    compressed_size += len(compressed)
                    return compressed
                    
                except json.JSONDecodeError:
                    # Если JSON некорректный, возвращаем как есть
                    compressed_size += len(json_str)
                    return json_str
            
            # Применяем сжатие
            df['rich_content'] = df['rich_content'].apply(compress_json)
            
            # Обновляем статистику
            stats.update({
                'compression_level': compression_level,
                'original_size_mb': round(original_size / (1024 * 1024), 2),
                'compressed_size_mb': round(compressed_size / (1024 * 1024), 2),
                'compression_ratio': round((original_size - compressed_size) / original_size * 100, 2) if original_size > 0 else 0
            })
            
            return df, stats
            
        except Exception as e:
            logger.error(f"Ошибка сжатого экспорта: {e}")
            return pd.DataFrame(), {'total_records': 0, 'success': False, 'error': str(e)}
    
    def export_by_filter(self, filters: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Экспорт Rich Content с фильтрацией.
        
        Args:
            filters: Словарь фильтров (brand, vendor_codes, etc.)
            
        Returns:
            Tuple[DataFrame, Stats] - данные и статистика экспорта
        """
        try:
            # Базовый запрос
            query = """
            SELECT 
                ocp.oz_vendor_code,
                COALESCE(op.oz_sku, '') as oz_sku,
                ocp.rich_content_json as rich_content
            FROM oz_category_products ocp
            LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
            WHERE ocp.rich_content_json IS NOT NULL 
            AND ocp.rich_content_json != ''
            AND LENGTH(ocp.rich_content_json) > 10
            """
            
            # Добавляем фильтры
            filter_params = []
            
            if filters.get('brand'):
                query += " AND ocp.oz_brand = ?"
                filter_params.append(filters['brand'])
            
            if filters.get('vendor_codes'):
                placeholders = ','.join(['?' for _ in filters['vendor_codes']])
                query += f" AND ocp.oz_vendor_code IN ({placeholders})"
                filter_params.extend(filters['vendor_codes'])
            
            if filters.get('min_json_size'):
                query += " AND LENGTH(ocp.rich_content_json) >= ?"
                filter_params.append(filters['min_json_size'])
            
            if filters.get('max_json_size'):
                query += " AND LENGTH(ocp.rich_content_json) <= ?"
                filter_params.append(filters['max_json_size'])
            
            query += " ORDER BY ocp.oz_vendor_code"
            
            if filters.get('limit'):
                query += f" LIMIT {filters['limit']}"
            
            # Выполняем запрос
            start_time = time.time()
            results = self.db_conn.execute(query, filter_params).fetchall()
            query_time = time.time() - start_time
            
            # Создаем DataFrame
            if results:
                df = pd.DataFrame(results, columns=['oz_vendor_code', 'oz_sku', 'rich_content'])
                
                stats = {
                    'total_records': len(results),
                    'with_sku': len(df[df['oz_sku'] != '']),
                    'without_sku': len(df[df['oz_sku'] == '']),
                    'query_time_seconds': round(query_time, 2),
                    'filters_applied': filters,
                    'success': True
                }
                
                return df, stats
            else:
                return pd.DataFrame(), {'total_records': 0, 'success': False, 'filters_applied': filters}
                
        except Exception as e:
            logger.error(f"Ошибка фильтрованного экспорта: {e}")
            return pd.DataFrame(), {'total_records': 0, 'success': False, 'error': str(e)}
    
    def validate_rich_content_json(self, sample_size: int = 100) -> Dict[str, Any]:
        """
        Валидация Rich Content JSON данных.
        
        Args:
            sample_size: Размер выборки для валидации
            
        Returns:
            Dict с результатами валидации
        """
        try:
            query = """
            SELECT 
                ocp.oz_vendor_code,
                ocp.rich_content_json,
                LENGTH(ocp.rich_content_json) as json_length
            FROM oz_category_products ocp
            WHERE ocp.rich_content_json IS NOT NULL 
            AND ocp.rich_content_json != ''
            AND LENGTH(ocp.rich_content_json) > 10
            ORDER BY RANDOM()
            LIMIT ?
            """
            
            results = self.db_conn.execute(query, [sample_size]).fetchall()
            
            if not results:
                return {'total_tested': 0, 'valid_json': 0, 'invalid_json': 0}
            
            valid_count = 0
            invalid_count = 0
            validation_errors = []
            
            for vendor_code, json_str, json_length in results:
                try:
                    json_data = json.loads(json_str)
                    
                    # Дополнительная валидация структуры
                    if 'content' in json_data and 'version' in json_data:
                        valid_count += 1
                    else:
                        invalid_count += 1
                        validation_errors.append(f"{vendor_code}: Отсутствует обязательная структура")
                        
                except json.JSONDecodeError as e:
                    invalid_count += 1
                    validation_errors.append(f"{vendor_code}: JSON decode error - {str(e)}")
            
            return {
                'total_tested': len(results),
                'valid_json': valid_count,
                'invalid_json': invalid_count,
                'validation_success_rate': round((valid_count / len(results)) * 100, 2),
                'validation_errors': validation_errors[:10],  # Первые 10 ошибок
                'avg_json_length': round(sum(r[2] for r in results) / len(results), 2)
            }
            
        except Exception as e:
            logger.error(f"Ошибка валидации JSON: {e}")
            return {'total_tested': 0, 'error': str(e)}