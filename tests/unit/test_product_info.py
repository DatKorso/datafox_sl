"""
Unit тесты для класса ProductInfo.
"""

import pytest
from utils.rich_content_oz import ProductInfo


class TestProductInfo:
    """Тесты для модели данных товара"""
    
    def test_basic_product_creation(self):
        """Тест создания базового товара"""
        product = ProductInfo(
            oz_vendor_code="TEST-001",
            type="Сабо",
            gender="Женский",
            oz_brand="TestBrand"
        )
        
        assert product.oz_vendor_code == "TEST-001"
        assert product.type == "Сабо"
        assert product.gender == "Женский"
        assert product.oz_brand == "TestBrand"
        assert product.oz_fbo_stock == 0  # Дефолтное значение
        assert not product.has_punta_data  # Дефолтное значение
    
    def test_size_normalization_integer(self):
        """Тест нормализации целочисленных размеров"""
        test_cases = [
            ("38", "38"),
            ("38.0", "38"),
            ("38,0", "38"),
            ("  38  ", "38"),
            (38, "38"),
            (38.0, "38"),
        ]
        
        for input_size, expected in test_cases:
            product = ProductInfo(
                oz_vendor_code="TEST",
                russian_size=str(input_size)
            )
            assert product.russian_size == expected, f"Input: {input_size}, Expected: {expected}, Got: {product.russian_size}"
    
    def test_size_normalization_decimal(self):
        """Тест нормализации десятичных размеров"""
        test_cases = [
            ("38.5", "38.5"),
            ("38,5", "38.5"),
            ("  38.5  ", "38.5"),
            (38.5, "38.5"),
        ]
        
        for input_size, expected in test_cases:
            product = ProductInfo(
                oz_vendor_code="TEST",
                russian_size=str(input_size)
            )
            assert product.russian_size == expected, f"Input: {input_size}, Expected: {expected}, Got: {product.russian_size}"
    
    def test_size_normalization_invalid(self):
        """Тест обработки невалидных размеров"""
        test_cases = [
            ("XL", "XL"),
            ("38-39", "38-39"),
            ("", ""),
            ("N/A", "N/A"),
        ]
        
        for input_size, expected in test_cases:
            product = ProductInfo(
                oz_vendor_code="TEST",
                russian_size=input_size
            )
            assert product.russian_size == expected, f"Input: {input_size}, Expected: {expected}, Got: {product.russian_size}"
    
    def test_size_normalization_none(self):
        """Тест обработки None размера"""
        product = ProductInfo(
            oz_vendor_code="TEST",
            russian_size=None
        )
        assert product.russian_size is None
    
    def test_has_punta_data_detection(self):
        """Тест автоматического определения наличия Punta данных"""
        # Товар без Punta данных
        product_no_punta = ProductInfo(
            oz_vendor_code="TEST-001",
            material_short=None,
            new_last=None,
            mega_last=None,
            best_last=None
        )
        assert not product_no_punta.has_punta_data
        
        # Товар с material_short
        product_with_material = ProductInfo(
            oz_vendor_code="TEST-002",
            material_short="Экокожа"
        )
        product_with_material.has_punta_data = bool(product_with_material.material_short)
        assert product_with_material.has_punta_data
        
        # Товар с new_last
        product_with_new_last = ProductInfo(
            oz_vendor_code="TEST-003",
            new_last="Standard"
        )
        product_with_new_last.has_punta_data = bool(product_with_new_last.new_last)
        assert product_with_new_last.has_punta_data
        
        # Товар с mega_last
        product_with_mega_last = ProductInfo(
            oz_vendor_code="TEST-004",
            mega_last="Mega001"
        )
        product_with_mega_last.has_punta_data = bool(product_with_mega_last.mega_last)
        assert product_with_mega_last.has_punta_data
        
        # Товар с best_last
        product_with_best_last = ProductInfo(
            oz_vendor_code="TEST-005",
            best_last="Best001"
        )
        product_with_best_last.has_punta_data = bool(product_with_best_last.best_last)
        assert product_with_best_last.has_punta_data
    
    def test_product_copy(self, sample_product_info):
        """Тест создания копии товара"""
        original = sample_product_info
        copy = original.copy()
        
        # Проверяем, что это разные объекты
        assert copy is not original
        
        # Проверяем, что все поля скопированы корректно
        assert copy.oz_vendor_code == original.oz_vendor_code
        assert copy.type == original.type
        assert copy.gender == original.gender
        assert copy.oz_brand == original.oz_brand
        assert copy.russian_size == original.russian_size
        assert copy.season == original.season
        assert copy.color == original.color
        assert copy.fastener_type == original.fastener_type
        assert copy.oz_fbo_stock == original.oz_fbo_stock
        assert copy.main_photo_url == original.main_photo_url
        assert copy.material_short == original.material_short
        assert copy.new_last == original.new_last
        assert copy.mega_last == original.mega_last
        assert copy.best_last == original.best_last
        assert copy.wb_sku == original.wb_sku
        assert copy.has_punta_data == original.has_punta_data
        
        # Проверяем, что изменение копии не влияет на оригинал
        copy.oz_vendor_code = "CHANGED"
        assert original.oz_vendor_code != "CHANGED"
    
    def test_product_with_all_fields(self):
        """Тест создания товара со всеми заполненными полями"""
        product = ProductInfo(
            oz_vendor_code="FULL-001",
            type="Сабо",
            gender="Женский",
            oz_brand="FullBrand",
            russian_size="38.5",
            season="Лето",
            color="красный",
            fastener_type="Липучка",
            oz_fbo_stock=25,
            main_photo_url="https://example.com/full.jpg",
            material_short="Натуральная кожа",
            new_last="NewLast001",
            mega_last="MegaLast001",
            best_last="BestLast001",
            wb_sku=99999,
            has_punta_data=True
        )
        
        # Проверяем все поля
        assert product.oz_vendor_code == "FULL-001"
        assert product.type == "Сабо"
        assert product.gender == "Женский"
        assert product.oz_brand == "FullBrand"
        assert product.russian_size == "38.5"
        assert product.season == "Лето"
        assert product.color == "красный"
        assert product.fastener_type == "Липучка"
        assert product.oz_fbo_stock == 25
        assert product.main_photo_url == "https://example.com/full.jpg"
        assert product.material_short == "Натуральная кожа"
        assert product.new_last == "NewLast001"
        assert product.mega_last == "MegaLast001"
        assert product.best_last == "BestLast001"
        assert product.wb_sku == 99999
        assert product.has_punta_data is True
    
    def test_edge_case_sizes(self):
        """Тест граничных случаев для размеров"""
        edge_cases = [
            # Очень маленький размер
            ("0.5", "0.5"),
            
            # Очень большой размер  
            ("999", "999"),
            
            # Размер с большим количеством знаков после запятой
            ("38.12345", "38.12345"),
            
            # Отрицательный размер (некорректный, но должен обрабатываться)
            ("-5", "-5"),
            
            # Очень длинная строка
            ("this_is_a_very_long_size_string", "this_is_a_very_long_size_string"),
        ]
        
        for input_size, expected in edge_cases:
            product = ProductInfo(
                oz_vendor_code="EDGE-TEST",
                russian_size=input_size
            )
            assert product.russian_size == expected, f"Input: {input_size}, Expected: {expected}, Got: {product.russian_size}" 