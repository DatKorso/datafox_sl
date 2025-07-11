"""
Конфигурация pytest и общие fixtures для тестирования модуля Rich Content.
"""

import pytest
import tempfile
import duckdb
import os
import sys
from typing import Generator, List
from unittest.mock import Mock

# Добавляем путь к корневой директории проекта для импорта модулей
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from utils.rich_content_oz import (
    ProductInfo, ScoringConfig, ProcessingStatus, 
    Recommendation, ProductDataCollector
)


@pytest.fixture
def sample_product_info() -> ProductInfo:
    """Базовый образец товара для тестирования"""
    return ProductInfo(
        oz_vendor_code="TEST-001",
        type="Сабо",
        gender="Женский",
        oz_brand="TestBrand",
        russian_size="38",
        season="Лето",
        color="белый",
        fastener_type="Без застёжки",
        oz_fbo_stock=10,
        main_photo_url="https://example.com/photo.jpg",
        material_short="Экокожа",
        new_last="Standard",
        mega_last=None,
        best_last=None,
        wb_sku=12345,
        has_punta_data=True
    )


@pytest.fixture
def default_scoring_config() -> ScoringConfig:
    """Базовая конфигурация scoring для тестов"""
    return ScoringConfig()


@pytest.fixture
def test_db() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Создание тестовой базы данных с образцами данных"""
    # Создаем временную БД в памяти
    conn = duckdb.connect(':memory:')
    
    try:
        # Создание схемы тестовых таблиц
        _create_test_schema(conn)
        
        # Заполнение тестовыми данными
        _populate_test_data(conn)
        
        yield conn
        
    finally:
        conn.close()


def _create_test_schema(conn: duckdb.DuckDBPyConnection):
    """Создание схемы тестовых таблиц"""
    
    # Таблица категорийных товаров Ozon
    conn.execute("""
        CREATE TABLE oz_category_products (
            oz_vendor_code VARCHAR PRIMARY KEY,
            type VARCHAR,
            gender VARCHAR,
            oz_brand VARCHAR,
            russian_size VARCHAR,
            season VARCHAR,
            color VARCHAR,
            fastener_type VARCHAR,
            main_photo_url VARCHAR
        )
    """)
    
    # Таблица товаров Ozon (остатки)
    conn.execute("""
        CREATE TABLE oz_products (
            oz_vendor_code VARCHAR PRIMARY KEY,
            oz_fbo_stock INTEGER DEFAULT 0
        )
    """)
    
    # Таблица штрихкодов Ozon
    conn.execute("""
        CREATE TABLE oz_barcodes (
            oz_vendor_code VARCHAR,
            oz_barcode VARCHAR,
            PRIMARY KEY (oz_vendor_code, oz_barcode)
        )
    """)
    
    # Таблица товаров WB
    conn.execute("""
        CREATE TABLE wb_products (
            wb_sku INTEGER PRIMARY KEY,
            wb_barcodes VARCHAR
        )
    """)
    
    # Справочная таблица Punta
    conn.execute("""
        CREATE TABLE punta_table (
            wb_sku INTEGER PRIMARY KEY,
            material_short VARCHAR,
            new_last VARCHAR,
            mega_last VARCHAR,
            best_last VARCHAR
        )
    """)


def _populate_test_data(conn: duckdb.DuckDBPyConnection):
    """Заполнение тестовыми данными"""
    
    # Основной товар для тестирования рекомендаций
    conn.execute("""
        INSERT INTO oz_category_products VALUES 
        ('BASE-001', 'Сабо', 'Женский', 'TestBrand', '38', 'Лето', 'белый', 'Без застёжки', 'https://example.com/base.jpg')
    """)
    
    conn.execute("""
        INSERT INTO oz_products VALUES ('BASE-001', 5)
    """)
    
    conn.execute("""
        INSERT INTO oz_barcodes VALUES ('BASE-001', '1234567890123')
    """)
    
    # Похожие товары для рекомендаций
    similar_products = [
        # Идеальное совпадение (кроме цвета)
        ('SIM-001', 'Сабо', 'Женский', 'TestBrand', '38', 'Лето', 'черный', 'Без застёжки', 'https://example.com/sim1.jpg', 10, '1234567890124'),
        
        # Близкий размер (+1)
        ('SIM-002', 'Сабо', 'Женский', 'TestBrand', '39', 'Лето', 'белый', 'Без застёжки', 'https://example.com/sim2.jpg', 15, '1234567890125'),
        
        # Другой сезон
        ('SIM-003', 'Сабо', 'Женский', 'TestBrand', '38', 'Весна', 'белый', 'Без застёжки', 'https://example.com/sim3.jpg', 3, '1234567890126'),
        
        # Низкий остаток
        ('SIM-004', 'Сабо', 'Женский', 'TestBrand', '38', 'Лето', 'белый', 'Шнуровка', 'https://example.com/sim4.jpg', 1, '1234567890127'),
        
        # Большая разница в размере (+3)
        ('SIM-005', 'Сабо', 'Женский', 'TestBrand', '41', 'Лето', 'белый', 'Без застёжки', 'https://example.com/sim5.jpg', 8, '1234567890128'),
    ]
    
    # Товары, которые НЕ должны попасть в рекомендации
    different_products = [
        # Другой тип
        ('DIFF-001', 'Ботинки', 'Женский', 'TestBrand', '38', 'Лето', 'белый', 'Шнуровка', 'https://example.com/diff1.jpg', 8, '1234567890129'),
        
        # Другой пол  
        ('DIFF-002', 'Сабо', 'Мужской', 'TestBrand', '38', 'Лето', 'белый', 'Без застёжки', 'https://example.com/diff2.jpg', 12, '1234567890130'),
        
        # Другой бренд
        ('DIFF-003', 'Сабо', 'Женский', 'OtherBrand', '38', 'Лето', 'белый', 'Без застёжки', 'https://example.com/diff3.jpg', 6, '1234567890131'),
        
        # Нет остатков
        ('DIFF-004', 'Сабо', 'Женский', 'TestBrand', '38', 'Лето', 'белый', 'Без застёжки', 'https://example.com/diff4.jpg', 0, '1234567890132'),
    ]
    
    all_products = similar_products + different_products
    
    for code, type_, gender, brand, size, season, color, fastener, photo, stock, barcode in all_products:
        # Вставляем в основные таблицы
        conn.execute("""
            INSERT INTO oz_category_products VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [code, type_, gender, brand, size, season, color, fastener, photo])
        
        conn.execute("""
            INSERT INTO oz_products VALUES (?, ?)
        """, [code, stock])
        
        conn.execute("""
            INSERT INTO oz_barcodes VALUES (?, ?)
        """, [code, barcode])
    
    # Создаем связи с WB и Punta для некоторых товаров
    wb_connections = [
        # wb_sku, barcode, material_short, new_last, mega_last, best_last
        (10001, '1234567890123', 'Экокожа', 'Standard', None, None),        # BASE-001
        (10002, '1234567890124', 'Экокожа', 'Standard', None, None),        # SIM-001
        (10003, '1234567890125', 'Кожа', None, 'Mega001', None),            # SIM-002
        (10004, '1234567890126', 'Экокожа', None, None, 'Best001'),         # SIM-003
    ]
    
    for wb_sku, barcode, material, new_last, mega_last, best_last in wb_connections:
        # WB Products
        conn.execute("""
            INSERT INTO wb_products VALUES (?, ?)
        """, [wb_sku, barcode])
        
        # Punta Table
        conn.execute("""
            INSERT INTO punta_table VALUES (?, ?, ?, ?, ?)
        """, [wb_sku, material, new_last, mega_last, best_last])


@pytest.fixture
def sample_recommendations(sample_product_info) -> List[Recommendation]:
    """Образцы рекомендаций для тестирования"""
    recommendations = []
    
    # Создаем несколько тестовых рекомендаций с разными score
    test_cases = [
        ("REC-001", 450, "Идеальное совпадение"),
        ("REC-002", 380, "Хорошее совпадение"),  
        ("REC-003", 250, "Среднее совпадение"),
        ("REC-004", 120, "Слабое совпадение"),
    ]
    
    for vendor_code, score, details in test_cases:
        product = sample_product_info.copy()
        product.oz_vendor_code = vendor_code
        
        recommendation = Recommendation(
            product_info=product,
            score=score,
            match_details=details,
            processing_status=ProcessingStatus.SUCCESS
        )
        recommendations.append(recommendation)
    
    return recommendations


@pytest.fixture
def data_collector(test_db) -> ProductDataCollector:
    """ProductDataCollector с подключенной тестовой БД"""
    return ProductDataCollector(test_db)


# Маркеры для pytest
pytest_plugins = [] 

@pytest.fixture
def mock_db_connection():
    """Мок подключения к базе данных для тестирования"""
    mock_conn = Mock()
    
    # Настройка базовых методов
    mock_conn.execute.return_value = Mock()
    mock_conn.execute.return_value.fetchone.return_value = None
    mock_conn.execute.return_value.fetchall.return_value = []
    
    return mock_conn 