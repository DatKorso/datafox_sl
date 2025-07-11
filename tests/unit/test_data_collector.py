"""
Unit тесты для класса ProductDataCollector.
"""

import pytest
from unittest.mock import Mock, patch
from utils.rich_content_oz import ProductDataCollector, ProductInfo


class TestProductDataCollector:
    """Тесты для сбора данных о товарах из БД"""
    
    def test_get_full_product_info_basic(self, data_collector):
        """Тест получения базовой информации о товаре"""
        product_info = data_collector.get_full_product_info('BASE-001')
        
        assert product_info is not None
        assert isinstance(product_info, ProductInfo)
        assert product_info.oz_vendor_code == 'BASE-001'
        assert product_info.type == 'Сабо'
        assert product_info.gender == 'Женский'
        assert product_info.oz_brand == 'TestBrand'
        assert product_info.russian_size == '38'
        assert product_info.season == 'Лето'
        assert product_info.color == 'белый'
        assert product_info.fastener_type == 'Без застёжки'
        assert product_info.oz_fbo_stock == 5
    
    def test_get_full_product_info_with_punta_data(self, data_collector):
        """Тест получения товара с данными из punta_table"""
        # В тестовых данных BASE-001 должен иметь связь с punta_table
        product_info = data_collector.get_full_product_info('BASE-001')
        
        assert product_info is not None
        # Проверяем, что есть связь с punta данными
        # В зависимости от тестовых данных из conftest.py
        if product_info.wb_sku:
            assert product_info.wb_sku == 10001
            assert product_info.material_short == 'Экокожа'
            assert product_info.new_last == 'Standard'
            assert product_info.has_punta_data is True
    
    def test_get_full_product_info_nonexistent(self, data_collector):
        """Тест получения несуществующего товара"""
        product_info = data_collector.get_full_product_info('NONEXISTENT-001')
        
        assert product_info is None
    
    def test_caching_mechanism(self, data_collector):
        """Тест работы кэширования"""
        # Первый вызов - должен попасть в БД
        product_info1 = data_collector.get_full_product_info('BASE-001')
        
        # Второй вызов - должен вернуться из кэша
        product_info2 = data_collector.get_full_product_info('BASE-001')
        
        # Результаты должны быть идентичными
        assert product_info1 is not None
        assert product_info2 is not None
        assert product_info1.oz_vendor_code == product_info2.oz_vendor_code
        assert product_info1.type == product_info2.type
        
        # Проверяем, что объекты одинаковые (из кэша)
        assert product_info1 is product_info2
    
    def test_cache_clear(self, data_collector):
        """Тест очистки кэша"""
        # Заполняем кэш
        product_info1 = data_collector.get_full_product_info('BASE-001')
        assert product_info1 is not None
        
        # Очищаем кэш
        data_collector.clear_cache()
        
        # Следующий вызов должен снова обратиться к БД
        product_info2 = data_collector.get_full_product_info('BASE-001')
        assert product_info2 is not None
        
        # Объекты должны быть разными (новый создан после очистки кэша)
        assert product_info1 is not product_info2
        # Но данные должны быть одинаковыми
        assert product_info1.oz_vendor_code == product_info2.oz_vendor_code
    
    def test_find_similar_products_candidates_basic(self, data_collector, sample_product_info):
        """Тест поиска кандидатов для рекомендаций"""
        # Используем BASE-001 как исходный товар
        source_product = data_collector.get_full_product_info('BASE-001')
        assert source_product is not None
        
        candidates = data_collector.find_similar_products_candidates(source_product)
        
        # Должны найтись кандидаты
        assert len(candidates) > 0
        
        # Все кандидаты должны иметь те же обязательные параметры
        for candidate in candidates:
            assert candidate.type == source_product.type
            assert candidate.gender == source_product.gender
            assert candidate.oz_brand == source_product.oz_brand
            assert candidate.oz_vendor_code != source_product.oz_vendor_code  # Исключаем сам товар
            assert candidate.oz_fbo_stock > 0  # Только товары в наличии
    
    def test_find_similar_products_candidates_filtering(self, data_collector):
        """Тест фильтрации кандидатов по обязательным критериям"""
        source_product = data_collector.get_full_product_info('BASE-001')
        assert source_product is not None
        
        candidates = data_collector.find_similar_products_candidates(source_product)
        
        # Проверяем, что отфильтрованы товары с другими критериями
        candidate_codes = [c.oz_vendor_code for c in candidates]
        
        # Не должно быть товаров с другим типом (DIFF-001 - Ботинки)
        assert 'DIFF-001' not in candidate_codes
        
        # Не должно быть товаров с другим полом (DIFF-002 - Мужской)  
        assert 'DIFF-002' not in candidate_codes
        
        # Не должно быть товаров с другим брендом (DIFF-003 - OtherBrand)
        assert 'DIFF-003' not in candidate_codes
        
        # Не должно быть товаров без остатков (DIFF-004 - stock=0)
        assert 'DIFF-004' not in candidate_codes
    
    def test_find_similar_products_incomplete_source(self, data_collector):
        """Тест поиска кандидатов для товара с неполными данными"""
        # Создаем товар с неполными обязательными данными
        incomplete_product = ProductInfo(
            oz_vendor_code="INCOMPLETE-001",
            type="Сабо",
            gender=None,  # Отсутствует обязательное поле
            oz_brand="TestBrand"
        )
        
        candidates = data_collector.find_similar_products_candidates(incomplete_product)
        
        # Не должно найтись кандидатов из-за неполных данных
        assert len(candidates) == 0
    
    def test_database_error_handling(self):
        """Тест обработки ошибок базы данных"""
        # Создаем mock connection, который вызывает ошибку
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Database connection error")
        
        collector = ProductDataCollector(mock_conn)
        
        # Должен обработать ошибку и вернуть None
        result = collector.get_full_product_info('TEST-001')
        assert result is None
    
    def test_empty_result_handling(self):
        """Тест обработки пустых результатов из БД"""
        # Создаем mock connection, который возвращает None
        mock_conn = Mock()
        mock_conn.execute.return_value.fetchone.return_value = None
        
        collector = ProductDataCollector(mock_conn)
        
        result = collector.get_full_product_info('NONEXISTENT-001')
        assert result is None
    
    def test_data_type_conversion(self, data_collector):
        """Тест корректного преобразования типов данных из БД"""
        product_info = data_collector.get_full_product_info('BASE-001')
        assert product_info is not None
        
        # Проверяем типы данных
        assert isinstance(product_info.oz_vendor_code, str)
        assert isinstance(product_info.oz_fbo_stock, int)
        
        # Опциональные поля могут быть None или соответствующим типом
        if product_info.wb_sku is not None:
            assert isinstance(product_info.wb_sku, int)
        
        if product_info.has_punta_data is not None:
            assert isinstance(product_info.has_punta_data, bool)
    
    def test_sql_injection_protection(self, data_collector):
        """Тест защиты от SQL инъекций"""
        # Пытаемся передать потенциально опасный код
        malicious_code = "'; DROP TABLE oz_category_products; --"
        
        # Метод должен обработать это безопасно и вернуть None
        result = data_collector.get_full_product_info(malicious_code)
        assert result is None
        
        # Проверяем, что таблица всё ещё существует
        normal_result = data_collector.get_full_product_info('BASE-001')
        assert normal_result is not None
    
    def test_multiple_candidates_order(self, data_collector):
        """Тест того, что кандидаты возвращаются в стабильном порядке"""
        source_product = data_collector.get_full_product_info('BASE-001')
        assert source_product is not None
        
        # Получаем кандидатов несколько раз
        candidates1 = data_collector.find_similar_products_candidates(source_product)
        candidates2 = data_collector.find_similar_products_candidates(source_product)
        
        # Порядок должен быть стабильным
        assert len(candidates1) == len(candidates2)
        for i in range(len(candidates1)):
            assert candidates1[i].oz_vendor_code == candidates2[i].oz_vendor_code
    
    def test_product_info_normalization(self, data_collector):
        """Тест нормализации данных товара при создании ProductInfo"""
        product_info = data_collector.get_full_product_info('BASE-001')
        assert product_info is not None
        
        # Размер должен быть нормализован
        assert product_info.russian_size == '38'  # Без десятичных знаков для целого числа
        
        # Остальные поля должны быть очищены от лишних пробелов
        assert product_info.type.strip() == product_info.type
        assert product_info.gender.strip() == product_info.gender
        assert product_info.oz_brand.strip() == product_info.oz_brand
    
    @patch('utils.rich_content_oz.logger')
    def test_logging_on_error(self, mock_logger):
        """Тест логирования ошибок"""
        # Создаем mock connection, который вызывает ошибку
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Test error")
        
        collector = ProductDataCollector(mock_conn)
        result = collector.get_full_product_info('TEST-001')
        
        # Проверяем, что ошибка была залогирована
        assert result is None
        mock_logger.error.assert_called_once()
        
        # Проверяем содержание лога
        call_args = mock_logger.error.call_args[0][0]
        assert "TEST-001" in call_args
        assert "Test error" in call_args
    
    @patch('utils.rich_content_oz.logger')
    def test_logging_on_missing_product(self, mock_logger, data_collector):
        """Тест логирования предупреждений для отсутствующих товаров"""
        result = data_collector.get_full_product_info('MISSING-001')
        
        assert result is None
        mock_logger.warning.assert_called_once()
        
        # Проверяем содержание предупреждения
        call_args = mock_logger.warning.call_args[0][0]
        assert "MISSING-001" in call_args
        assert "не найден" in call_args 