"""
Rich Content Processor v2 - Упрощенная версия для пакетной обработки.

Основные принципы:
- Минимализм - только необходимый функционал
- Переиспользование существующего кода
- Оптимизация для пакетной обработки
- Структурированное логирование
"""

import time
import logging
from typing import List, Dict, Any, Callable, Optional
from dataclasses import dataclass

from .rich_content_oz import (
    ScoringConfig, 
    RichContentProcessor, 
    ProcessingResult
)

logger = logging.getLogger(__name__)

@dataclass
class BatchResult:
    """Результат обработки одного товара в пакете"""
    oz_vendor_code: str
    rich_content_json: Optional[str] = None
    recommendations_count: int = 0
    processing_time: float = 0.0
    error_message: Optional[str] = None
    success: bool = False

class BatchProcessorV2:
    """
    Упрощенный процессор для пакетной обработки товаров v2.
    
    Основные отличия от оригинала:
    - Фокус только на пакетной обработке
    - Возврат результатов вместо сохранения в БД
    - Упрощенная структура данных
    - Оптимизированное логирование
    """
    
    def __init__(self, connection, config: ScoringConfig):
        """
        Инициализация процессора.
        
        Args:
            connection: Подключение к базе данных
            config: Конфигурация алгоритма рекомендаций
        """
        self.connection = connection
        self.config = config
        
        # Создаем основной процессор (переиспользуем существующий код)
        self.processor = RichContentProcessor(connection, config)
        
        logger.info("BatchProcessorV2 инициализирован")
    
    def process_vendor_codes_list(
        self, 
        vendor_codes: List[str], 
        progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> List[Dict[str, Any]]:
        """
        Обработка списка артикулов товаров.
        
        Args:
            vendor_codes: Список артикулов для обработки
            progress_callback: Callback для обновления прогресса (current, total, message)
            
        Returns:
            Список результатов обработки
        """
        logger.info(f"Начинаем пакетную обработку {len(vendor_codes)} товаров")
        
        results = []
        start_time = time.time()
        
        for i, vendor_code in enumerate(vendor_codes, 1):
            # Callback прогресса
            if progress_callback:
                progress_callback(i-1, len(vendor_codes), f"Обрабатываем {vendor_code}")
            
            # Обработка одного товара
            result = self._process_single_vendor_code(vendor_code)
            results.append(result)
            
            # Логирование прогресса
            if i % 10 == 0 or i == len(vendor_codes):
                elapsed = time.time() - start_time
                avg_time = elapsed / i
                logger.info(f"Обработано {i}/{len(vendor_codes)} товаров. "
                           f"Средняя скорость: {avg_time:.2f}с/товар")
        
        # Финальный callback
        if progress_callback:
            progress_callback(len(vendor_codes), len(vendor_codes), "Обработка завершена")
        
        total_time = time.time() - start_time
        successful = len([r for r in results if r.get('success')])
        
        logger.info(f"Пакетная обработка завершена: {successful}/{len(vendor_codes)} "
                   f"успешных за {total_time:.1f}с")
        
        return results
    
    def _process_single_vendor_code(self, vendor_code: str) -> Dict[str, Any]:
        """
        Обработка одного артикула товара.
        
        Args:
            vendor_code: Артикул товара
            
        Returns:
            Словарь с результатом обработки
        """
        start_time = time.time()
        
        try:
            logger.debug(f"Обрабатываем товар: {vendor_code}")
            
            # Используем существующий процессор
            processing_result: ProcessingResult = self.processor.process_single_product(vendor_code)
            
            processing_time = time.time() - start_time
            
            # Формируем результат с детализацией scoring
            scoring_details = self._extract_scoring_details(processing_result.recommendations)
            
            result = {
                'oz_vendor_code': vendor_code,
                'rich_content_json': processing_result.rich_content_json,
                'recommendations_count': len(processing_result.recommendations),
                'processing_time': processing_time,
                'success': processing_result.success,
                'error_message': processing_result.error_message,
                'scoring_details': scoring_details
            }
            
            if processing_result.success:
                logger.debug(f"Товар {vendor_code} обработан успешно: "
                           f"{len(processing_result.recommendations)} рекомендаций за {processing_time:.2f}с")
            else:
                logger.warning(f"Ошибка обработки товара {vendor_code}: {processing_result.error_message}")
            
            return result
            
        except Exception as e:
            processing_time = time.time() - start_time
            error_msg = f"Критическая ошибка: {str(e)}"
            
            logger.error(f"Критическая ошибка при обработке {vendor_code}: {e}")
            
            return {
                'oz_vendor_code': vendor_code,
                'rich_content_json': None,
                'recommendations_count': 0,
                'processing_time': processing_time,
                'success': False,
                'error_message': error_msg,
                'scoring_details': f"Ошибка: {error_msg}"
            }
    
    def get_statistics(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Получение статистики по результатам обработки.
        
        Args:
            results: Список результатов обработки
            
        Returns:
            Словарь со статистикой
        """
        if not results:
            return {
                'total_processed': 0,
                'successful': 0,
                'failed': 0,
                'success_rate': 0.0,
                'avg_processing_time': 0.0,
                'total_recommendations': 0,
                'avg_recommendations_per_product': 0.0
            }
        
        successful_results = [r for r in results if r.get('success')]
        failed_results = [r for r in results if not r.get('success')]
        
        total_processing_time = sum(r.get('processing_time', 0) for r in results)
        total_recommendations = sum(r.get('recommendations_count', 0) for r in successful_results)
        
        stats = {
            'total_processed': len(results),
            'successful': len(successful_results),
            'failed': len(failed_results),
            'success_rate': len(successful_results) / len(results) * 100,
            'avg_processing_time': total_processing_time / len(results),
            'total_recommendations': total_recommendations,
            'avg_recommendations_per_product': (
                total_recommendations / len(successful_results) 
                if successful_results else 0
            )
        }
        
        logger.info(f"Статистика обработки: {stats}")
        return stats
    
    def validate_vendor_codes(self, vendor_codes: List[str]) -> Dict[str, List[str]]:
        """
        Валидация списка артикулов перед обработкой.
        
        Args:
            vendor_codes: Список артикулов для валидации
            
        Returns:
            Словарь с валидными и невалидными артикулами
        """
        valid_codes = []
        invalid_codes = []
        
        for code in vendor_codes:
            # Базовая валидация
            if isinstance(code, str) and len(code.strip()) > 2:
                valid_codes.append(code.strip())
            else:
                invalid_codes.append(code)
        
        logger.info(f"Валидация артикулов: {len(valid_codes)} валидных, {len(invalid_codes)} невалидных")
        
        if invalid_codes:
            logger.warning(f"Невалидные артикулы: {invalid_codes}")
        
        return {
            'valid': valid_codes,
            'invalid': invalid_codes
        }
    
    def _extract_scoring_details(self, recommendations: List) -> str:
        """
        Извлечение детализированной информации о scoring из рекомендаций.
        
        Args:
            recommendations: Список рекомендаций
            
        Returns:
            Строка с детализацией scoring
        """
        if not recommendations:
            return "Нет рекомендаций"
        
        details = []
        details.append(f"Рекомендаций: {len(recommendations)}")
        
        # Анализируем score диапазон
        scores = [rec.score for rec in recommendations]
        if scores:
            details.append(f"Score: {min(scores):.1f}-{max(scores):.1f}")
        
        # Анализируем первую рекомендацию для примера совпадений
        if recommendations:
            first_rec = recommendations[0]
            match_details = first_rec.match_details
            
            # Извлекаем ключевые совпадения из match_details
            matches = []
            penalties = []
            
            if "✓" in match_details:
                match_lines = [line.strip() for line in match_details.split('\n') if '✓' in line]
                for line in match_lines[:3]:  # Первые 3 совпадения
                    if 'Размер:' in line:
                        matches.append('размер')
                    elif 'Сезон:' in line:
                        matches.append('сезон')
                    elif 'Цвет:' in line:
                        matches.append('цвет')
                    elif 'Материал:' in line:
                        matches.append('материал')
                    elif 'Модель:' in line:
                        matches.append('модель')
                    elif 'Колодка:' in line:
                        matches.append('колодка')
                    elif 'Застежка:' in line:
                        matches.append('застежка')
            
            if "штраф" in match_details.lower():
                penalty_lines = [line.strip() for line in match_details.split('\n') if 'штраф' in line.lower()]
                for line in penalty_lines:
                    if 'колодка' in line.lower():
                        penalties.append('колодка')
                    elif 'сезон' in line.lower():
                        penalties.append('сезон')
            
            if matches:
                details.append(f"Совпадения: {', '.join(matches)}")
            if penalties:
                details.append(f"Штрафы: {', '.join(penalties)}")
        
        return " | ".join(details)
