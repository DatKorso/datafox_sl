# Cross-Marketplace Linker - Модуль связывания артикулов

**Файл:** `utils/cross_marketplace_linker.py`  
**Версия:** 1.0.0  
**Статус:** ✅ Реализован  
**Назначение:** Централизованное управление связями между артикулами WB и Ozon через штрихкоды

## 🎯 Цель модуля

Исключить дублирование логики связывания `wb_sku` ↔ `oz_sku`, которая используется в **7+ местах** проекта:

1. **`cards_matcher_helpers.py`** - `get_wb_sku_ratings_with_oz_data()`
2. **`pages/6_📊_Статистика_Заказов_OZ.py`** - `get_linked_ozon_skus_for_wb_sku()`
3. **`pages/9_🔄_Сверка_Категорий_OZ.py`** - `find_linked_products_with_categories()`
4. **`pages/7_🎯_Менеджер_Рекламы_OZ.py`** - `get_linked_ozon_skus_with_details()`
5. **`analytic_report_helpers.py`** - `map_wb_to_ozon_by_size()`
6. **`existing_groups_helpers.py`** - `get_group_details_with_wb_connections()`
7. **`utils/db_search_helpers.py`** - `find_cross_marketplace_matches()` (используется в `pages/5_🔎_Поиск_Между_МП.py`)

## 🏗️ Архитектура

### Основной класс: `CrossMarketplaceLinker`

```python
from utils.cross_marketplace_linker import CrossMarketplaceLinker

# Инициализация
linker = CrossMarketplaceLinker(db_connection)

# Основные методы
wb_to_oz = linker.link_wb_to_oz(['123456', '789012'])
oz_to_wb = linker.link_oz_to_wb(['456789', '321654'])
full_links = linker.get_bidirectional_links(wb_skus=['123456'])
```

### Ключевые методы

#### 1. **Базовое связывание**
```python
def link_wb_to_oz(wb_skus: List[str]) -> Dict[str, List[str]]
def link_oz_to_wb(oz_skus: List[str]) -> Dict[str, List[str]]
```

#### 2. **Расширенные связи**
```python
def get_bidirectional_links(wb_skus=None, oz_skus=None) -> pd.DataFrame
def get_links_with_ozon_ratings(wb_skus: List[str]) -> pd.DataFrame
def get_links_with_categories(input_skus: List[str], search_by="wb_sku") -> pd.DataFrame
```

#### 3. **Служебные методы**
```python
def _normalize_and_merge_barcodes() -> pd.DataFrame  # Центральная логика
def clear_cache()  # Очистка кэша
```

## 🚀 Преимущества

### 1. **Производительность**
- **Кэширование** результатов на 5 минут через `@st.cache_data(ttl=300)`
- **Оптимизированные запросы** - используются проверенные функции из `db_search_helpers.py`
- **Ленивая загрузка** - данные загружаются только при необходимости

### 2. **Надежность**
- **Единообразная обработка ошибок** во всех методах
- **Валидация входных данных** - проверка типов и очистка
- **Защита от пустых данных** - graceful handling

### 3. **Поддерживаемость**  
- **Единое место** для всей логики связывания
- **Документированный API** с типами
- **Расширяемость** - легко добавлять новые типы связывания

## 📋 API Reference

### Основные функции

#### `link_wb_to_oz(wb_skus: List[str]) -> Dict[str, List[str]]`
**Назначение:** Находит связанные Ozon SKU для заданных WB SKU

**Входные данные:**
- `wb_skus` - список артикулов WB

**Возврат:**
```python
{
    "123456": ["789012", "789013"],
    "123457": ["789014"]
}
```

**Кэширование:** ✅ 5 минут

---

#### `get_bidirectional_links(wb_skus=None, oz_skus=None) -> pd.DataFrame`
**Назначение:** Полная таблица связей с деталями

**Возврат:**
| wb_sku | oz_sku | oz_vendor_code | oz_product_id | common_barcode |
|--------|--------|----------------|---------------|----------------|
| 123456 | 789012 | VENDOR_001     | 55123         | 4607034571234  |

---

#### `get_links_with_ozon_ratings(wb_skus: List[str]) -> pd.DataFrame`
**Назначение:** Связи с агрегированными рейтингами Ozon

**Возврат:**
| wb_sku | avg_rating | oz_sku_count | oz_skus_list | total_reviews | min_rating | max_rating |
|--------|------------|--------------|--------------|---------------|------------|------------|
| 123456 | 4.5        | 2            | 789012,789013| 150           | 4.0        | 5.0        |

**Заменяет:** `get_wb_sku_ratings_with_oz_data()` из `cards_matcher_helpers.py`

---

#### `find_marketplace_matches()` - **НОВЫЙ МЕТОД**
**Назначение:** Универсальный поиск между маркетплейсами с выбираемыми полями

**Заменяет:** `find_cross_marketplace_matches()` из `utils/db_search_helpers.py`

```python
def find_marketplace_matches(
    self, 
    search_criterion: str,
    search_values: List[str], 
    fields_to_display: List[str]
) -> pd.DataFrame:
    """
    Универсальный поиск между маркетплейсами.
    
    Args:
        search_criterion: 'wb_sku', 'oz_sku', 'oz_vendor_code', 'barcode'
        search_values: Список значений для поиска
        fields_to_display: Поля для отображения в результатах
        
    Returns:
        DataFrame с результатами поиска
    """
```

## 🔄 План миграции

### Этап 1: Базовая замена (1-2 дня)
Заменить простые случаи использования:

```python
# ❌ Старый код
from utils.db_search_helpers import get_normalized_wb_barcodes, get_ozon_barcodes_and_identifiers

def get_linked_ozon_skus_for_wb_sku(db_conn, wb_sku_list):
    wb_barcodes_df = get_normalized_wb_barcodes(db_conn, wb_skus=wb_sku_list_str)
    oz_barcodes_ids_df = get_ozon_barcodes_and_identifiers(db_conn)
    # ... 20+ строк дублированной логики
    return linked_skus_map

# ✅ Новый код  
from utils.cross_marketplace_linker import get_wb_to_oz_links

def get_linked_ozon_skus_for_wb_sku(db_conn, wb_sku_list):
    return get_wb_to_oz_links(db_conn, wb_sku_list)
```

### Этап 2: Расширенная замена (3-5 дней)
Заменить сложные случаи с дополнительными данными:

```python
# ❌ Старый код в db_search_helpers.py (200+ строк)
def find_cross_marketplace_matches(con, search_criterion, search_values, selected_fields_map):
    # Много дублированной логики...

# ✅ Новый код (1 строка!)
def find_cross_marketplace_matches(con, search_criterion, search_values, selected_fields_map):
    linker = CrossMarketplaceLinker(con)
    return linker.find_marketplace_matches(search_criterion, search_values, selected_fields_map)
```

### Этап 3: Полная оптимизация (1 неделя)
- Добавить метрики производительности
- Улучшить кэширование
- Создать unit-тесты

## 📊 Обновленный список замен

### Готовые к замене функции:

| Файл | Функция | Метод замены | Сложность |
|------|---------|--------------|-----------|
| `cards_matcher_helpers.py` | `get_wb_sku_ratings_with_oz_data()` | `get_links_with_ozon_ratings()` | 🟢 Простая |
| `pages/6_📊_Статистика_Заказов_OZ.py` | `get_linked_ozon_skus_for_wb_sku()` | `link_wb_to_oz()` | ✅ **Выполнено** |
| `pages/7_🎯_Менеджер_Рекламы_OZ.py` | `get_linked_ozon_skus_with_details()` | `get_bidirectional_links()` | 🟡 Средняя |
| `pages/9_🔄_Сверка_Категорий_OZ.py` | `find_linked_products_with_categories()` | `get_links_with_categories()` | 🟡 Средняя |
| **`pages/5_🔎_Поиск_Между_МП.py`** | **`find_cross_marketplace_matches()`** | **`find_marketplace_matches()`** | **✅ Выполнено** |

### Требуют доработки:

| Файл | Функция | Что нужно | Статус |
|------|---------|-----------|--------|
| `analytic_report_helpers.py` | `map_wb_to_ozon_by_size()` | Добавить группировку по размерам | 🔄 Планируется |
| `existing_groups_helpers.py` | `get_group_details_with_wb_connections()` | Адаптировать под группы | 🔄 Планируется |

### **Новое: Критическая замена**
**`utils/db_search_helpers.py` → `find_cross_marketplace_matches()`**
- **Используется в:** `pages/5_🔎_Поиск_Между_МП.py` 
- **Сложность:** 🔴 Высокая (200+ строк логики)
- **Приоритет:** Высокий (основная функция поиска)

## 🧪 Тестирование

### Ручное тестирование
```python
# Тест базового функционала
from utils.cross_marketplace_linker import CrossMarketplaceLinker
from utils.db_connection import get_connection_and_ensure_schema

conn = get_connection_and_ensure_schema()
linker = CrossMarketplaceLinker(conn)

# Тест 1: Базовое связывание
wb_skus = ['123456', '789012']  # Замените на реальные SKU
result = linker.link_wb_to_oz(wb_skus)
print(f"WB -> Ozon links: {result}")

# Тест 2: Связи с рейтингами
ratings_df = linker.get_links_with_ozon_ratings(wb_skus)
print(f"Links with ratings: {len(ratings_df)} rows")

# Тест 3: Производительность кэша
import time
start = time.time()
result1 = linker.link_wb_to_oz(wb_skus)  # Первый запрос
mid = time.time()
result2 = linker.link_wb_to_oz(wb_skus)  # Кэшированный запрос
end = time.time()

print(f"First call: {mid-start:.3f}s, Cached call: {end-mid:.3f}s")

# Тест 4: Универсальный поиск (НОВЫЙ)
search_fields = ['wb_sku', 'oz_sku', 'common_barcode']
search_result = linker.find_marketplace_matches('wb_sku', wb_skus, search_fields)
print(f"Universal search: {len(search_result)} matches")
```

## 📈 Обновленные ожидаемые результаты

### Сокращение кода
- **Удаление ~400 строк** дублированной логики (+100 строк от find_cross_marketplace_matches)
- **Упрощение** функций в 7+ файлах
- **Стандартизация** обработки ошибок

### Улучшение производительности
- **Кэширование** - ускорение повторных запросов в 5-10 раз
- **Оптимизация** - использование проверенных запросов
- **Снижение нагрузки** на БД при повторных обращениях

### Повышение надежности
- **Единое место** для багфиксов и улучшений
- **Контролируемое** тестирование изменений
- **Последовательная** обработка edge cases

---

**Статус внедрения:** 🟡 Готов к использованию, ожидает миграции  
**Следующий шаг:** Добавление метода `find_marketplace_matches()` и обновление плана миграции 