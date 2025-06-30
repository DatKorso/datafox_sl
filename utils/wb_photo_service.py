"""
Wildberries Photo Service Module

Предоставляет функции для получения ссылок на изображения товаров WB по артикулу.
Использует алгоритм построения прямых ссылок на CDN WB.

Пример использования:
    from utils.wb_photo_service import get_wb_photo_url, validate_wb_photo_url
    
    # Получить ссылку на фото
    photo_url = get_wb_photo_url('297266982')
    
    # Проверить доступность фото
    is_available = validate_wb_photo_url('297266982')
"""

import requests
from typing import Optional, Tuple
import logging

# Настройка логирования
logger = logging.getLogger(__name__)

def get_wb_photo_url(wb_sku: str) -> str:
    """
    Генерирует прямую ссылку на изображение товара WB по артикулу.
    
    Алгоритм основан на структуре CDN Wildberries:
    - Артикул конвертируется в число
    - Вычисляются значения для определения тома и части
    - Формируется URL на основе этих значений
    
    Args:
        wb_sku (str): Артикул товара WB (может быть строкой или числом)
        
    Returns:
        str: Прямая ссылка на изображение или пустая строка при ошибке
        
    Example:
        >>> get_wb_photo_url('297266982')
        'https://basket-04.wbbasket.ru/vol2972/part297266/297266982/images/tm/1.webp'
    """
    try:
        # Конвертируем в целое число
        a = int(float(wb_sku))
        
        # Вычисляем b и c согласно алгоритму WB
        b = a // 100000
        c = a // 1000
        
        # Определяем номер тома (vol_num) по диапазонам
        vol_num = _get_volume_number(b)
        
        # Формируем финальный URL
        image_url = f"https://basket-{vol_num}.wbbasket.ru/vol{b}/part{c}/{a}/images/tm/1.webp"
        
        logger.debug(f"Generated URL for WB_SKU {wb_sku}: {image_url}")
        return image_url
        
    except (ValueError, TypeError) as e:
        logger.warning(f"Не удалось сгенерировать URL изображения для WB_SKU {wb_sku}: {e}")
        return ""
    except Exception as e:
        logger.error(f"Неожиданная ошибка при генерации URL для WB_SKU {wb_sku}: {e}")
        return ""

def _get_volume_number(b: int) -> str:
    """
    Определяет номер тома CDN на основе значения b.
    
    Args:
        b (int): Значение b = wb_sku // 100000
        
    Returns:
        str: Номер тома в формате "01"-"25"
    """
    if b <= 143:
        return "01"
    elif b <= 287:
        return "02"
    elif b <= 431:
        return "03"
    elif b <= 719:
        return "04"
    elif b <= 1007:
        return "05"
    elif b <= 1061:
        return "06"
    elif b <= 1115:
        return "07"
    elif b <= 1169:
        return "08"
    elif b <= 1313:
        return "09"
    elif b <= 1601:
        return "10"
    elif b <= 1655:
        return "11"
    elif b <= 1919:
        return "12"
    elif b <= 2045:
        return "13"
    elif b <= 2189:
        return "14"
    elif b <= 2405:
        return "15"
    elif b <= 2621:
        return "16"
    elif b <= 2837:
        return "17"
    elif b <= 3053:
        return "18"
    elif b <= 3269:
        return "19"
    elif b <= 3485:
        return "20"
    elif b <= 3701:
        return "21"
    elif b <= 3917:
        return "22"
    elif b <= 4133:
        return "23"
    elif b <= 4349:
        return "24"
    else:
        return "25"

def validate_wb_photo_url(wb_sku: str, timeout: int = 10) -> Tuple[bool, str]:
    """
    Проверяет доступность изображения товара WB по артикулу.
    
    Args:
        wb_sku (str): Артикул товара WB
        timeout (int): Таймаут запроса в секундах (по умолчанию 10)
        
    Returns:
        Tuple[bool, str]: (доступно, URL или сообщение об ошибке)
        
    Example:
        >>> is_available, url = validate_wb_photo_url('297266982')
        >>> if is_available:
        ...     print(f"Фото доступно: {url}")
        ... else:
        ...     print(f"Ошибка: {url}")
    """
    image_url = get_wb_photo_url(wb_sku)
    
    if not image_url:
        return False, "Не удалось сгенерировать URL"
    
    try:
        # Используем headers для имитации браузера
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        # Делаем HEAD запрос для проверки доступности без загрузки изображения
        response = requests.head(image_url, timeout=timeout, headers=headers)
        
        if response.status_code == 200:
            # Проверяем что это действительно изображение
            content_type = response.headers.get('content-type', '')
            if content_type.startswith('image/'):
                logger.debug(f"Image available for WB_SKU {wb_sku}: {image_url}")
                return True, image_url
            else:
                error_msg = f"URL не содержит изображение (content-type: {content_type})"
                logger.warning(f"WB_SKU {wb_sku}: {error_msg}")
                return False, error_msg
        else:
            error_msg = f"HTTP {response.status_code}: {response.reason}"
            logger.warning(f"Image not available for WB_SKU {wb_sku}: {error_msg}")
            return False, error_msg
            
    except requests.exceptions.Timeout:
        error_msg = f"Таймаут запроса ({timeout}s)"
        logger.warning(f"WB_SKU {wb_sku}: {error_msg}")
        return False, error_msg
    except requests.exceptions.RequestException as e:
        error_msg = f"Ошибка сети: {str(e)}"
        logger.warning(f"WB_SKU {wb_sku}: {error_msg}")
        return False, error_msg
    except Exception as e:
        error_msg = f"Неожиданная ошибка: {str(e)}"
        logger.error(f"WB_SKU {wb_sku}: {error_msg}")
        return False, error_msg

def get_wb_photo_info(wb_sku: str, validate: bool = True, timeout: int = 10) -> dict:
    """
    Получает полную информацию о фотографии товара WB.
    
    Args:
        wb_sku (str): Артикул товара WB
        validate (bool): Проверять ли доступность изображения (по умолчанию True)
        timeout (int): Таймаут для проверки в секундах (по умолчанию 10)
        
    Returns:
        dict: Словарь с информацией о фотографии:
            {
                'wb_sku': str,           # Исходный артикул
                'url': str,              # Сгенерированный URL
                'available': bool,       # Доступно ли изображение (если validate=True)
                'error': str,            # Сообщение об ошибке (если есть)
                'validated': bool        # Была ли выполнена проверка доступности
            }
            
    Example:
        >>> info = get_wb_photo_info('297266982')
        >>> print(f"URL: {info['url']}")
        >>> print(f"Доступно: {info['available']}")
    """
    result = {
        'wb_sku': wb_sku,
        'url': '',
        'available': False,
        'error': '',
        'validated': validate
    }
    
    # Генерируем URL
    url = get_wb_photo_url(wb_sku)
    result['url'] = url
    
    if not url:
        result['error'] = 'Не удалось сгенерировать URL'
        return result
    
    # Проверяем доступность если требуется
    if validate:
        is_available, error_or_url = validate_wb_photo_url(wb_sku, timeout)
        result['available'] = is_available
        if not is_available:
            result['error'] = error_or_url
    else:
        # Если не проверяем, считаем доступным
        result['available'] = True
    
    return result

# Функции для совместимости с существующим кодом
def get_wb_image_url(wb_sku: str) -> str:
    """
    Алиас для get_wb_photo_url для совместимости с существующим кодом.
    
    Args:
        wb_sku (str): Артикул товара WB
        
    Returns:
        str: Прямая ссылка на изображение
    """
    return get_wb_photo_url(wb_sku)

if __name__ == "__main__":
    # Тестирование модуля
    test_sku = "297266982"
    
    print(f"Тестирование WB Photo Service с артикулом: {test_sku}")
    print("-" * 50)
    
    # Тест 1: Генерация URL
    url = get_wb_photo_url(test_sku)
    print(f"Сгенерированный URL: {url}")
    
    # Тест 2: Проверка доступности
    is_available, result = validate_wb_photo_url(test_sku)
    print(f"Доступность: {is_available}")
    print(f"Результат: {result}")
    
    # Тест 3: Полная информация
    info = get_wb_photo_info(test_sku)
    print(f"Полная информация: {info}")