"""
Unit тесты для класса RichContentProcessor.
"""

import pytest
import json
from unittest.mock import Mock, patch
from utils.rich_content_oz import (
    RichContentProcessor, ProductInfo, Recommendation, ProcessingResult,
    ProcessingStatus, BatchResult, ScoringConfig, RICH_CONTENT_VERSION
)


class TestRichContentProcessor:
    """Тесты для процессора Rich Content"""
    
    @pytest.fixture
    def processor(self, mock_db_connection, default_scoring_config):
        """Создание процессора Rich Content"""
        return RichContentProcessor(mock_db_connection, default_scoring_config)
    
    @pytest.fixture
    def mock_recommendations(self, sample_recommendations):
        """Мок-рекомендации для тестирования"""
        return sample_recommendations[:3]  # Используем первые 3 рекомендации
    
    def test_processor_initialization(self, mock_db_connection, default_scoring_config):
        """Тест инициализации процессора"""
        processor = RichContentProcessor(mock_db_connection, default_scoring_config)
        
        assert processor.db_conn == mock_db_connection
        assert processor.config == default_scoring_config
        assert processor.recommendation_engine is not None
        assert processor.content_generator is not None
    
    def test_processor_initialization_default_config(self, mock_db_connection):
        """Тест инициализации процессора с конфигом по умолчанию"""
        processor = RichContentProcessor(mock_db_connection)
        
        assert processor.config is not None
        assert isinstance(processor.config, ScoringConfig)
    
    @patch('utils.rich_content_oz.RecommendationEngine.find_similar_products')
    def test_process_single_product_success(self, mock_find_similar, processor, mock_recommendations):
        """Тест успешной обработки одного товара"""
        # Настройка мока
        mock_find_similar.return_value = mock_recommendations
        
        # Обработка товара
        result = processor.process_single_product("TEST-001")
        
        # Проверяем результат
        assert isinstance(result, ProcessingResult)
        assert result.oz_vendor_code == "TEST-001"
        assert result.status == ProcessingStatus.SUCCESS
        assert len(result.recommendations) == 3
        assert result.rich_content_json is not None
        assert result.processing_time > 0
        assert result.error_message is None
        
        # Проверяем, что JSON валидный
        data = json.loads(result.rich_content_json)
        assert 'content' in data
        assert 'version' in data
        
        # Проверяем, что был вызван поиск рекомендаций
        mock_find_similar.assert_called_once_with("TEST-001")
    
    @patch('utils.rich_content_oz.RecommendationEngine.find_similar_products')
    def test_process_single_product_no_similar(self, mock_find_similar, processor):
        """Тест обработки товара без похожих товаров"""
        # Мок возвращает пустой список
        mock_find_similar.return_value = []
        
        # Обработка товара
        result = processor.process_single_product("TEST-002")
        
        # Проверяем результат
        assert result.status == ProcessingStatus.NO_SIMILAR
        assert len(result.recommendations) == 0
        assert result.rich_content_json is not None
        
        # Проверяем содержимое JSON (должно быть сообщение о том, что нет рекомендаций)
        data = json.loads(result.rich_content_json)
        content_text = data['content'][0]['text']['content']
        assert 'нет рекомендаций' in content_text
    
    @patch('utils.rich_content_oz.RecommendationEngine.find_similar_products')
    def test_process_single_product_error(self, mock_find_similar, processor):
        """Тест обработки ошибки при поиске рекомендаций"""
        # Мок выбрасывает исключение
        mock_find_similar.side_effect = Exception("Database error")
        
        # Обработка товара
        result = processor.process_single_product("TEST-003")
        
        # Проверяем результат
        assert result.status == ProcessingStatus.ERROR
        assert result.error_message == "Database error"
        assert len(result.recommendations) == 0
    
    @patch('utils.rich_content_oz.RichContentGenerator.validate_rich_content_json')
    @patch('utils.rich_content_oz.RecommendationEngine.find_similar_products')
    def test_process_single_product_validation_error(self, mock_find_similar, mock_validate, 
                                                    processor, mock_recommendations):
        """Тест обработки ошибки валидации JSON"""
        # Настройка моков
        mock_find_similar.return_value = mock_recommendations
        mock_validate.return_value = False  # Валидация не прошла
        
        # Обработка товара
        result = processor.process_single_product("TEST-004")
        
        # Проверяем результат
        assert result.status == ProcessingStatus.ERROR
        assert "Ошибка валидации Rich Content JSON" in result.error_message
        assert len(result.recommendations) == 3  # Рекомендации есть, но JSON невалидный
    
    @patch('utils.rich_content_oz.RecommendationEngine.find_similar_products')
    def test_process_batch_success(self, mock_find_similar, processor, mock_recommendations):
        """Тест успешной пакетной обработки"""
        # Мок возвращает рекомендации для всех товаров
        mock_find_similar.return_value = mock_recommendations
        
        vendor_codes = ["TEST-001", "TEST-002", "TEST-003"]
        
        # Пакетная обработка
        batch_result = processor.process_batch(vendor_codes)
        
        # Проверяем результат
        assert isinstance(batch_result, BatchResult)
        assert batch_result.total_items == 3
        assert len(batch_result.processed_items) == 3
        
        # Проверяем статистику
        stats = batch_result.stats
        assert stats['successful'] == 3
        assert stats['no_similar'] == 0
        assert stats['errors'] == 0
        
        # Проверяем, что все товары обработаны успешно
        for result in batch_result.processed_items:
            assert result.status == ProcessingStatus.SUCCESS
            assert len(result.recommendations) == 3
        
        # Проверяем количество вызовов поиска
        assert mock_find_similar.call_count == 3
    
    @patch('utils.rich_content_oz.RecommendationEngine.find_similar_products')
    def test_process_batch_mixed_results(self, mock_find_similar, processor, mock_recommendations):
        """Тест пакетной обработки со смешанными результатами"""
        # Настройка мока для разных результатов
        def side_effect(vendor_code):
            if vendor_code == "TEST-001":
                return mock_recommendations  # Успех
            elif vendor_code == "TEST-002":
                return []  # Нет похожих
            else:
                raise Exception("Error")  # Ошибка
        
        mock_find_similar.side_effect = side_effect
        
        vendor_codes = ["TEST-001", "TEST-002", "TEST-003"]
        
        # Пакетная обработка
        batch_result = processor.process_batch(vendor_codes)
        
        # Проверяем статистику
        stats = batch_result.stats
        assert stats['successful'] == 1
        assert stats['no_similar'] == 1
        assert stats['errors'] == 1
        
        # Проверяем статусы результатов
        statuses = [result.status for result in batch_result.processed_items]
        assert ProcessingStatus.SUCCESS in statuses
        assert ProcessingStatus.NO_SIMILAR in statuses
        assert ProcessingStatus.ERROR in statuses
    
    def test_process_batch_with_progress_callback(self, processor):
        """Тест пакетной обработки с callback прогресса"""
        progress_calls = []
        
        def progress_callback(current, total, message):
            progress_calls.append((current, total, message))
        
        with patch('utils.rich_content_oz.RecommendationEngine.find_similar_products') as mock_find:
            mock_find.return_value = []
            
            vendor_codes = ["TEST-001", "TEST-002"]
            processor.process_batch(vendor_codes, progress_callback)
        
        # Проверяем вызовы callback
        assert len(progress_calls) == 3  # 2 товара + финальный вызов
        
        # Проверяем первый вызов
        current, total, message = progress_calls[0]
        assert current == 1
        assert total == 2
        assert "TEST-001" in message
        
        # Проверяем финальный вызов
        final_current, final_total, final_message = progress_calls[-1]
        assert final_current == 2
        assert final_total == 2
        assert "завершена" in final_message
    
    def test_process_batch_empty_list(self, processor):
        """Тест пакетной обработки пустого списка"""
        batch_result = processor.process_batch([])
        
        assert batch_result.total_items == 0
        assert len(batch_result.processed_items) == 0
        
        stats = batch_result.stats
        assert stats['successful'] == 0
        assert stats['no_similar'] == 0
        assert stats['errors'] == 0
    
    def test_save_rich_content_to_database_success(self, processor, mock_recommendations):
        """Тест успешного сохранения Rich Content в БД"""
        # Создаем успешный результат обработки
        processing_result = ProcessingResult(
            oz_vendor_code="TEST-001",
            status=ProcessingStatus.SUCCESS,
            recommendations=mock_recommendations,
            rich_content_json='{"content": [], "version": 0.3}',
            processing_time=1.0
        )
        
        # Сохранение в БД
        success = processor.save_rich_content_to_database(processing_result)
        
        # Проверяем результат
        assert success is True
        
        # Проверяем, что была выполнена команда UPDATE
        processor.db_conn.execute.assert_called_once()
        call_args = processor.db_conn.execute.call_args
        sql, params = call_args[0]
        
        assert "UPDATE oz_category_products" in sql
        assert "SET rich_content_json = ?" in sql
        assert "WHERE oz_vendor_code = ?" in sql
        assert params[0] == '{"content": [], "version": 0.3}'
        assert params[1] == "TEST-001"
    
    def test_save_rich_content_to_database_unsuccessful_result(self, processor):
        """Тест сохранения неуспешного результата (должно быть отклонено)"""
        # Создаем неуспешный результат обработки
        processing_result = ProcessingResult(
            oz_vendor_code="TEST-002",
            status=ProcessingStatus.ERROR,
            recommendations=[],
            error_message="Some error",
            processing_time=0.5
        )
        
        # Попытка сохранения в БД
        success = processor.save_rich_content_to_database(processing_result)
        
        # Проверяем, что сохранение отклонено
        assert success is False
        
        # Проверяем, что SQL не выполнялся
        processor.db_conn.execute.assert_not_called()
    
    def test_save_rich_content_to_database_sql_error(self, processor, mock_recommendations):
        """Тест обработки ошибки SQL при сохранении"""
        # Настройка мока для выброса исключения
        processor.db_conn.execute.side_effect = Exception("SQL Error")
        
        processing_result = ProcessingResult(
            oz_vendor_code="TEST-003",
            status=ProcessingStatus.SUCCESS,
            recommendations=mock_recommendations,
            rich_content_json='{"content": [], "version": 0.3}',
            processing_time=1.0
        )
        
        # Попытка сохранения в БД
        success = processor.save_rich_content_to_database(processing_result)
        
        # Проверяем, что сохранение не удалось
        assert success is False
    
    def test_get_processing_statistics_with_data(self, processor):
        """Тест получения статистики обработки с данными"""
        # Настройка мока для возврата тестовых данных
        mock_result = (100, 60, 40)  # total, with_content, without_content
        processor.db_conn.execute.return_value.fetchone.return_value = mock_result
        
        # Получение статистики
        stats = processor.get_processing_statistics()
        
        # Проверяем результат
        assert stats['total_products'] == 100
        assert stats['products_with_rich_content'] == 60
        assert stats['products_without_rich_content'] == 40
        assert stats['coverage_percent'] == 60.0
        
        # Проверяем SQL запрос
        processor.db_conn.execute.assert_called_once()
        sql = processor.db_conn.execute.call_args[0][0]
        assert "SELECT" in sql
        assert "COUNT(*)" in sql
        assert "oz_category_products" in sql
    
    def test_get_processing_statistics_empty_table(self, processor):
        """Тест получения статистики из пустой таблицы"""
        # Мок возвращает None (нет данных)
        processor.db_conn.execute.return_value.fetchone.return_value = None
        
        # Получение статистики
        stats = processor.get_processing_statistics()
        
        # Проверяем значения по умолчанию
        assert stats['total_products'] == 0
        assert stats['products_with_rich_content'] == 0
        assert stats['products_without_rich_content'] == 0
        assert stats['coverage_percent'] == 0.0
    
    def test_get_processing_statistics_sql_error(self, processor):
        """Тест обработки ошибки SQL при получении статистики"""
        # Настройка мока для выброса исключения
        processor.db_conn.execute.side_effect = Exception("SQL Error")
        
        # Получение статистики
        stats = processor.get_processing_statistics()
        
        # Проверяем, что возвращается информация об ошибке
        assert 'error' in stats
        assert stats['error'] == "SQL Error"
    
    def test_get_processing_statistics_coverage_calculation(self, processor):
        """Тест расчета покрытия в статистике"""
        # Тест случая с 0 общих товаров
        processor.db_conn.execute.return_value.fetchone.return_value = (0, 0, 0)
        stats = processor.get_processing_statistics()
        assert stats['coverage_percent'] == 0.0
        
        # Тест случая с частичным покрытием
        processor.db_conn.execute.return_value.fetchone.return_value = (150, 45, 105)
        stats = processor.get_processing_statistics()
        assert stats['coverage_percent'] == 30.0  # 45/150 * 100
        
        # Тест случая с полным покрытием
        processor.db_conn.execute.return_value.fetchone.return_value = (50, 50, 0)
        stats = processor.get_processing_statistics()
        assert stats['coverage_percent'] == 100.0
    
    @patch('utils.rich_content_oz.RecommendationEngine.find_similar_products')
    def test_integration_single_to_batch_processing(self, mock_find_similar, processor, mock_recommendations):
        """Интеграционный тест: от одного товара к пакетной обработке"""
        mock_find_similar.return_value = mock_recommendations
        
        # Сначала обрабатываем один товар
        single_result = processor.process_single_product("TEST-001")
        assert single_result.success
        
        # Затем тот же товар в пакете
        batch_result = processor.process_batch(["TEST-001"])
        assert len(batch_result.processed_items) == 1
        
        batch_item = batch_result.processed_items[0]
        
        # Проверяем, что результаты похожи (но не идентичны из-за времени обработки)
        assert batch_item.oz_vendor_code == single_result.oz_vendor_code
        assert batch_item.status == single_result.status
        assert len(batch_item.recommendations) == len(single_result.recommendations)
        
        # JSON должен быть одинаковый
        single_data = json.loads(single_result.rich_content_json)
        batch_data = json.loads(batch_item.rich_content_json)
        assert single_data == batch_data
    
    def test_performance_timing_measurement(self, processor):
        """Тест измерения времени обработки"""
        with patch('utils.rich_content_oz.RecommendationEngine.find_similar_products') as mock_find:
            # Мок с задержкой
            import time
            def slow_find(vendor_code):
                time.sleep(0.1)  # 100ms задержка
                return []
            
            mock_find.side_effect = slow_find
            
            result = processor.process_single_product("TEST-001")
            
            # Время обработки должно быть больше 0.1 секунды
            assert result.processing_time >= 0.1
    
    def test_batch_result_properties(self, processor):
        """Тест свойств и методов BatchResult"""
        with patch('utils.rich_content_oz.RecommendationEngine.find_similar_products') as mock_find:
            # Создаем разные типы результатов
            def side_effect(vendor_code):
                if vendor_code == "SUCCESS":
                    return [Mock()]  # Есть рекомендации
                elif vendor_code == "NO_SIMILAR":
                    return []  # Нет рекомендаций
                else:
                    raise Exception("Error")
            
            mock_find.side_effect = side_effect
            
            vendor_codes = ["SUCCESS", "NO_SIMILAR", "ERROR"]
            batch_result = processor.process_batch(vendor_codes)
            
            # Проверяем свойство success (хотя бы один успешный)
            assert batch_result.success  # Есть 1 успешный товар
            
            # Проверяем свойство all_successful
            assert not batch_result.all_successful  # Не все успешны
            
            # Проверяем статистику
            stats = batch_result.stats
            assert stats['successful'] == 1
            assert stats['no_similar'] == 1
            assert stats['errors'] == 1
            assert stats['success_rate'] == 33.33  # 1/3 * 100, округлено до 2 знаков 