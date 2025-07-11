"""
Unit тесты для класса RichContentGenerator.
"""

import pytest
import json
from utils.rich_content_oz import (
    RichContentGenerator, ProductInfo, Recommendation, 
    ScoringConfig, ProcessingStatus, RICH_CONTENT_VERSION
)


class TestRichContentGenerator:
    """Тесты для генератора Rich Content JSON"""
    
    @pytest.fixture
    def generator(self, default_scoring_config):
        """Создание генератора Rich Content"""
        return RichContentGenerator(default_scoring_config)
    
    @pytest.fixture
    def sample_recommendation(self):
        """Создание образца рекомендации"""
        product = ProductInfo(
            oz_vendor_code="REC-001",
            type="Сабо",
            gender="Женский",
            oz_brand="TestBrand",
            russian_size="38",
            season="Лето",
            color="белый",
            fastener_type="Без застёжки",
            oz_fbo_stock=10,
            main_photo_url="https://example.com/photo.jpg"
        )
        
        return Recommendation(
            product_info=product,
            score=450.0,
            match_details="Идеальное совпадение",
            processing_status=ProcessingStatus.SUCCESS
        )
    
    def test_generate_rich_content_json_basic(self, generator, sample_recommendation):
        """Тест базовой генерации Rich Content JSON"""
        recommendations = [sample_recommendation]
        
        json_string = generator.generate_rich_content_json(recommendations)
        
        # Проверяем, что результат - валидный JSON
        assert isinstance(json_string, str)
        data = json.loads(json_string)
        
        # Проверяем основную структуру
        assert 'content' in data
        assert 'version' in data
        assert data['version'] == RICH_CONTENT_VERSION
        assert isinstance(data['content'], list)
        assert len(data['content']) > 0
    
    def test_generate_rich_content_carousel_format(self, generator, sample_recommendation):
        """Тест генерации карусели рекомендаций"""
        recommendations = [sample_recommendation]
        
        json_string = generator.generate_rich_content_json(
            recommendations, 
            template_type="recommendations_carousel"
        )
        
        data = json.loads(json_string)
        content = data['content']
        
        # Должен содержать заголовок и карусель
        assert len(content) == 2
        
        # Проверяем заголовок
        title_widget = content[0]
        assert title_widget['widgetName'] == 'raText'
        assert '🔥 Похожие товары' in title_widget['text']['content']
        
        # Проверяем карусель
        carousel_widget = content[1]
        assert carousel_widget['widgetName'] == 'raShowcase'
        assert carousel_widget['type'] == 'roll'
        assert 'blocks' in carousel_widget
        assert len(carousel_widget['blocks']) == 1
    
    def test_generate_rich_content_grid_format(self, generator, sample_recommendations):
        """Тест генерации сетки рекомендаций"""
        json_string = generator.generate_rich_content_json(
            sample_recommendations[:4], 
            template_type="recommendations_grid"
        )
        
        data = json.loads(json_string)
        content = data['content']
        
        # Должен содержать заголовок и строки сетки
        assert len(content) >= 2
        
        # Проверяем заголовок
        title_widget = content[0]
        assert title_widget['widgetName'] == 'raText'
        assert '🎯 Рекомендуемые товары' in title_widget['text']['content']
        
        # Проверяем строки сетки
        grid_row = content[1]
        assert grid_row['widgetName'] == 'raColumns'
        assert 'columns' in grid_row
    
    def test_generate_rich_content_empty_recommendations(self, generator):
        """Тест генерации с пустым списком рекомендаций"""
        json_string = generator.generate_rich_content_json([])
        
        data = json.loads(json_string)
        content = data['content']
        
        # Должен создать сообщение о том, что нет рекомендаций
        assert len(content) == 1
        text_widget = content[0]
        assert text_widget['widgetName'] == 'raText'
        assert 'нет рекомендаций' in text_widget['text']['content']
    
    def test_generate_rich_content_unknown_template(self, generator, sample_recommendation):
        """Тест обработки неизвестного типа шаблона"""
        recommendations = [sample_recommendation]
        
        json_string = generator.generate_rich_content_json(
            recommendations, 
            template_type="unknown_template"
        )
        
        # Должен fallback к carousel
        data = json.loads(json_string)
        content = data['content']
        
        assert len(content) == 2
        carousel_widget = content[1]
        assert carousel_widget['widgetName'] == 'raShowcase'
    
    def test_create_product_block_structure(self, generator, sample_recommendation):
        """Тест структуры блока товара"""
        product_block = generator._create_product_block(sample_recommendation, 0)
        
        # Проверяем обязательные поля
        assert 'imgLink' in product_block
        assert 'img' in product_block
        assert 'title' in product_block
        assert 'subtitle' in product_block
        
        # Проверяем ссылку на товар
        assert product_block['imgLink'].startswith('https://www.ozon.ru/product/')
        assert 'REC-001' in product_block['imgLink']
        
        # Проверяем изображение
        img = product_block['img']
        assert img['src'] == 'https://example.com/photo.jpg'
        assert img['alt'] == 'Сабо TestBrand 38'
        
        # Проверяем заголовок
        assert product_block['title']['content'] == 'Сабо TestBrand'
        
        # Проверяем подзаголовок
        subtitle = product_block['subtitle']['content']
        assert 'Размер: 38' in subtitle
        assert 'В наличии: 10 шт.' in subtitle
        assert '⭐ 450' in subtitle
    
    def test_create_compact_product_block_structure(self, generator, sample_recommendation):
        """Тест структуры компактного блока товара"""
        compact_block = generator._create_compact_product_block(sample_recommendation)
        
        # Проверяем структуру
        assert compact_block['widgetName'] == 'raCard'
        assert 'link' in compact_block
        assert 'img' in compact_block
        assert 'content' in compact_block
        assert 'style' in compact_block
        
        # Проверяем содержимое
        content = compact_block['content']
        assert content['title'] == 'Сабо'
        assert content['subtitle'] == 'Размер: 38'
        assert content['badge'] == '⭐ 450'
        assert content['description'] == 'В наличии: 10 шт.'
    
    def test_format_product_details(self, generator):
        """Тест форматирования деталей товара"""
        product = ProductInfo(
            oz_vendor_code="TEST-001",
            russian_size="38",
            color="белый",
            oz_fbo_stock=5
        )
        
        details = generator._format_product_details(product, 250.0)
        
        # Проверяем наличие всех элементов
        assert 'Размер: 38' in details
        assert 'Цвет: белый' in details
        assert 'В наличии: 5 шт.' in details
        assert '⭐ 250' in details
        
        # Проверяем разделители
        assert ' • ' in details
    
    def test_format_product_details_partial_data(self, generator):
        """Тест форматирования с частичными данными"""
        product = ProductInfo(
            oz_vendor_code="TEST-001",
            russian_size="XL",
            color=None,  # Отсутствует цвет
            oz_fbo_stock=0  # Нет остатков
        )
        
        details = generator._format_product_details(product, 100.0)
        
        # Должен содержать только доступные данные
        assert 'Размер: XL' in details
        assert 'Цвет:' not in details
        assert 'В наличии:' not in details
        assert '⭐ 100' in details
    
    def test_generate_product_url(self, generator):
        """Тест генерации URL товара"""
        url = generator._generate_product_url("TEST-12345")
        
        assert url == "https://www.ozon.ru/product/TEST-12345/"
        assert url.startswith('https://www.ozon.ru/product/')
        assert url.endswith('/')
    
    def test_create_empty_content_structure(self, generator):
        """Тест структуры пустого контента"""
        empty_json = generator._create_empty_content()
        
        data = json.loads(empty_json)
        
        # Проверяем структуру
        assert 'content' in data
        assert 'version' in data
        assert data['version'] == RICH_CONTENT_VERSION
        
        # Проверяем содержимое
        content = data['content']
        assert len(content) == 1
        
        text_widget = content[0]
        assert text_widget['widgetName'] == 'raText'
        assert 'нет рекомендаций' in text_widget['text']['content']
    
    def test_validate_rich_content_json_valid(self, generator, sample_recommendation):
        """Тест валидации корректного Rich Content JSON"""
        recommendations = [sample_recommendation]
        json_string = generator.generate_rich_content_json(recommendations)
        
        is_valid = generator.validate_rich_content_json(json_string)
        
        assert is_valid is True
    
    def test_validate_rich_content_json_invalid_json(self, generator):
        """Тест валидации некорректного JSON"""
        invalid_json = '{"content": [invalid json}'
        
        is_valid = generator.validate_rich_content_json(invalid_json)
        
        assert is_valid is False
    
    def test_validate_rich_content_json_missing_content(self, generator):
        """Тест валидации JSON без поля content"""
        json_without_content = json.dumps({
            "version": RICH_CONTENT_VERSION
        })
        
        is_valid = generator.validate_rich_content_json(json_without_content)
        
        assert is_valid is False
    
    def test_validate_rich_content_json_missing_version(self, generator):
        """Тест валидации JSON без поля version"""
        json_without_version = json.dumps({
            "content": []
        })
        
        is_valid = generator.validate_rich_content_json(json_without_version)
        
        assert is_valid is False
    
    def test_validate_rich_content_json_invalid_content_type(self, generator):
        """Тест валидации JSON с некорректным типом content"""
        json_invalid_content = json.dumps({
            "content": "should be array",
            "version": RICH_CONTENT_VERSION
        })
        
        is_valid = generator.validate_rich_content_json(json_invalid_content)
        
        assert is_valid is False
    
    def test_validate_rich_content_json_invalid_widget_structure(self, generator):
        """Тест валидации JSON с некорректной структурой виджета"""
        json_invalid_widget = json.dumps({
            "content": [
                {
                    # Отсутствует widgetName
                    "text": {"content": "Test"}
                }
            ],
            "version": RICH_CONTENT_VERSION
        })
        
        is_valid = generator.validate_rich_content_json(json_invalid_widget)
        
        assert is_valid is False
    
    def test_carousel_max_items_limit(self, generator, sample_recommendations):
        """Тест ограничения количества товаров в карусели"""
        # Создаем 10 рекомендаций
        many_recommendations = sample_recommendations * 3  # 12 рекомендаций
        
        json_string = generator.generate_rich_content_json(
            many_recommendations, 
            template_type="recommendations_carousel"
        )
        
        data = json.loads(json_string)
        carousel_widget = data['content'][1]
        
        # Должно быть максимум столько товаров, сколько указано в конфигурации
        expected_max = generator.config.max_recommendations
        assert len(carousel_widget['blocks']) == expected_max
    
    def test_grid_max_items_limit(self, generator, sample_recommendations):
        """Тест ограничения количества товаров в сетке"""
        # Создаем 10 рекомендаций
        many_recommendations = sample_recommendations * 3  # 12 рекомендаций
        
        json_string = generator.generate_rich_content_json(
            many_recommendations, 
            template_type="recommendations_grid"
        )
        
        data = json.loads(json_string)
        content = data['content']
        
        # Заголовок + количество строк согласно конфигурации (по 2 товара в строке)
        expected_max = generator.config.max_recommendations
        expected_rows = (expected_max + 1) // 2  # Округление вверх для пар товаров
        assert len(content) <= expected_rows + 1  # title + grid rows
    
    def test_error_handling_in_generation(self, generator):
        """Тест обработки ошибок при генерации"""
        # Создаем рекомендацию с некорректными данными
        invalid_product = ProductInfo(oz_vendor_code=None)  # Некорректные данные
        invalid_recommendation = Recommendation(
            product_info=invalid_product,
            score=0,
            match_details=""
        )
        
        # Не должно вызывать исключений, должно вернуть empty content
        json_string = generator.generate_rich_content_json([invalid_recommendation])
        
        # Должен вернуть валидный JSON (возможно empty content)
        data = json.loads(json_string)
        assert 'content' in data
        assert 'version' in data
    
    def test_json_string_format(self, generator, sample_recommendation):
        """Тест формата выходной JSON строки"""
        recommendations = [sample_recommendation]
        json_string = generator.generate_rich_content_json(recommendations)
        
        # Проверяем, что JSON сжатый (используется compact separators)
        data = json.loads(json_string)
        compact_json = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
        assert json_string == compact_json
        
        # Проверяем кодировку (должна поддерживать русские символы)
        assert 'Похожие товары' in json_string
        assert 'TestBrand' in json_string 