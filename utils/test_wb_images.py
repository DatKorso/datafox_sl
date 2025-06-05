#!/usr/bin/env python3
"""
Тестовый скрипт для проверки функций работы с изображениями WB.
Можно запустить для проверки корректности алгоритма генерации URL и загрузки изображений.
"""

import sys
import os

# Добавляем путь к корню проекта для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.analytic_report_helpers import get_wb_image_url, download_wb_image

def test_url_generation():
    """Тестирует генерацию URL для различных артикулов WB."""
    
    test_cases = [
        ("12345", "basket-01"),     # Малый артикул
        ("1234567", "basket-01"),   # Средний артикул  
        ("98765432", "basket-01"),  # Большой артикул до 143*100000
        ("15000000", "basket-02"),  # Артикул для второго сервера
        ("50000000", "basket-04"),  # Артикул для четвертого сервера
        ("141944890", "basket-10"), # Конкретный пример из лога
        ("163509411", "basket-11"), # Еще пример из лога
        ("297267075", "basket-18"), # Третий пример из лога
        ("999999999", "basket-25")  # Очень большой артикул
    ]
    
    print("🧪 Тестирование генерации URL изображений:")
    print("-" * 60)
    
    for wb_sku, expected_server in test_cases:
        url = get_wb_image_url(wb_sku)
        print(f"WB SKU: {wb_sku:>10} → {url}")
        
        # Для конкретного примера показываем детальную проверку
        if wb_sku == "141944890":
            expected_url = "https://basket-10.wbbasket.ru/vol1419/part141944/141944890/images/tm/1.webp"
            print(f"{'':>25} Ожидался: {expected_url}")
            if url == expected_url:
                print(f"{'✅ URL ТОЧНО соответствует':>25}")
            else:
                print(f"{'❌ URL НЕ соответствует':>25}")
        
        # Проверяем что URL содержит ожидаемый сервер
        elif expected_server in url:
            print(f"{'✅ Сервер корректный':>25} ({expected_server})")
        else:
            print(f"{'❌ Ошибка сервера':>25} (ожидался {expected_server})")
        print()

def test_image_download(wb_sku: str = "141944890"):
    """Тестирует загрузку изображения для конкретного артикула."""
    
    print(f"🖼️ Тестирование загрузки изображения для WB SKU: {wb_sku}")
    print("-" * 60)
    
    # Генерируем URL
    url = get_wb_image_url(wb_sku)
    print(f"Сгенерированный URL: {url}")
    
    # Пытаемся загрузить изображение с увеличенным таймаутом
    image_data = download_wb_image(wb_sku, timeout=30)
    
    if image_data:
        print(f"✅ Изображение успешно загружено")
        print(f"   Размер: {len(image_data.getvalue())} байт")
        
        # Проверяем первые байты для определения формата
        image_data.seek(0)
        first_bytes = image_data.read(12)
        
        if first_bytes.startswith(b'RIFF') and b'WEBP' in first_bytes:
            print(f"   Формат: WebP ✅")
        elif first_bytes.startswith(b'\xFF\xD8\xFF'):
            print(f"   Формат: JPEG")
        elif first_bytes.startswith(b'\x89PNG'):
            print(f"   Формат: PNG")
        else:
            print(f"   Формат: Неизвестный ({first_bytes[:4].hex()})")
    else:
        print(f"❌ Не удалось загрузить изображение")

def test_multiple_downloads():
    """Тестирует загрузку изображений для нескольких артикулов из лога."""
    
    test_skus = ["141944890", "163509411", "297267075"]
    
    print("🖼️ Тестирование загрузки для артикулов из лога:")
    print("-" * 60)
    
    for sku in test_skus:
        print(f"\nТестирование WB SKU: {sku}")
        url = get_wb_image_url(sku)
        print(f"URL: {url}")
        
        image_data = download_wb_image(sku, timeout=30)
        if image_data:
            print(f"✅ Загружено {len(image_data.getvalue())} байт")
        else:
            print(f"❌ Загрузка не удалась")

def main():
    """Основная функция тестирования."""
    
    print("🔧 Тестирование функций работы с изображениями WB")
    print("=" * 70)
    print()
    
    # Тестируем генерацию URL
    test_url_generation()
    
    print()
    print("=" * 70)
    print()
    
    # Тестируем загрузку изображения
    test_wb_sku = sys.argv[1] if len(sys.argv) > 1 else "141944890"
    test_image_download(test_wb_sku)
    
    print()
    print("=" * 70)
    print()
    
    # Тестируем множественную загрузку
    test_multiple_downloads()

if __name__ == "__main__":
    main() 