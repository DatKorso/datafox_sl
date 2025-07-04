# Улучшенная группировка товаров v2.0.0

## Обзор

Этот модуль предоставляет исправленную версию функционала группировки товаров с корректной компенсацией рейтинга. Новая реализация решает основные проблемы оригинальной версии и предоставляет более надежный и предсказуемый алгоритм группировки.

## Основные проблемы оригинальной версии

### 1. Некорректные SQL запросы
- **Проблема**: Использование `INNER JOIN` с таблицей остатков исключало товары без остатков из поиска компенсаторов
- **Решение**: Замена на `LEFT JOIN` для корректного поиска всех подходящих товаров

### 2. Жадное распределение компенсаторов
- **Проблема**: Глобальный набор `used_wb_skus` приводил к захвату компенсаторов первыми обработанными группами
- **Решение**: Локальные пулы компенсаторов для каждой категории/пола с справедливым распределением

### 3. Отсутствие логирования
- **Проблема**: Невозможность отладки процесса поиска компенсаторов
- **Решение**: Детальное логирование всех этапов группировки

### 4. Архитектурные проблемы
- **Проблема**: Тесная связь UI и бизнес-логики в одном файле
- **Решение**: Разделение на отдельные модули с четким разделением ответственности

## Архитектура решения

### Модули

1. **`utils/advanced_product_grouper.py`** - Основная бизнес-логика
   - `GroupingConfig` - конфигурация группировки
   - `GroupingResult` - результат группировки
   - `AdvancedProductGrouper` - основной класс группировщика

2. **`utils/advanced_grouping_ui_components.py`** - UI компоненты
   - Функции рендеринга интерфейса
   - Обработка пользовательского ввода
   - Экспорт результатов

3. **`pages/11_🎯_Улучшенная_Группировка_Товаров.py`** - Streamlit страница
   - Интеграция UI и бизнес-логики
   - Обработка ошибок
   - Управление состоянием

## Ключевые улучшения

### 1. Исправленные SQL запросы

```sql
-- Оригинальная версия (неправильно)
SELECT p.wb_sku, p.avg_rating
FROM punta_data p
INNER JOIN stock_summary s ON p.wb_sku = s.wb_sku  -- Исключает товары без остатков!

-- Улучшенная версия (правильно)
SELECT p.wb_sku, p.avg_rating, COALESCE(s.total_stock, 0) as total_stock
FROM punta_data p
LEFT JOIN stock_summary s ON p.wb_sku = s.wb_sku  -- Включает все товары
```

### 2. Справедливое распределение компенсаторов

```python
# Оригинальная версия (проблематично)
used_wb_skus = set()  # Глобальный набор

# Улучшенная версия (справедливо)
compensator_pools = {
    ('Женский', 'Одежда'): [sku1, sku2, ...],
    ('Мужской', 'Обувь'): [sku3, sku4, ...]
}
```

### 3. Детальное логирование

```python
logs = [
    "[INFO] Начата обработка группы: gender=Женский, category=Одежда",
    "[INFO] Найдено 5 компенсаторов в пуле",
    "[WARNING] Группа требует 2 компенсатора, доступно 1",
    "[ERROR] Не удалось найти подходящие компенсаторы"
]
```

## Использование

### Базовый пример

```python
from utils.advanced_product_grouper import AdvancedProductGrouper, GroupingConfig
from utils.db_connection import connect_db

# Настройка
config = GroupingConfig(
    grouping_columns=['gender', 'wb_category'],
    min_group_rating=4.0,
    max_wb_sku_per_group=10
)

# Создание группировщика
conn = connect_db()
grouper = AdvancedProductGrouper(conn)

# Выполнение группировки
wb_skus = ['12345', '67890', '11111']
result = grouper.create_advanced_product_groups(wb_skus, config)

# Анализ результатов
print(f"Создано групп: {result.statistics['total_groups']}")
for group in result.groups:
    print(f"Группа {group['group_id']}: {group['item_count']} товаров")
```

### Конфигурация группировки

```python
config = GroupingConfig(
    grouping_columns=['gender', 'wb_category', 'brand'],  # Колонки для группировки
    min_group_rating=4.2,                                # Минимальный рейтинг группы
    max_wb_sku_per_group=15,                             # Максимум SKU в группе
    enable_sort_priority=True,                           # Использовать приоритет
    wb_category='Одежда'                                 # Фильтр по категории
)
```

## Структура результата

```python
result = GroupingResult(
    groups=[
        {
            'group_id': 'group_1',
            'group_rating': 4.3,
            'item_count': 8,
            'items': [
                {'wb_sku': '12345', 'avg_rating': 4.1, 'total_stock': 100},
                {'wb_sku': '67890', 'avg_rating': 4.5, 'total_stock': 50}
            ]
        }
    ],
    statistics={
        'total_groups': 5,
        'total_items_processed': 42,
        'avg_group_size': 8.4,
        'avg_group_rating': 4.2
    },
    logs=[
        "[INFO] Начата группировка 42 товаров",
        "[INFO] Создано 5 групп"
    ],
    low_rating_items=[],
    defective_items=[]
)
```

## Алгоритм работы

### 1. Подготовка данных
1. Загрузка данных Punta по WB SKU
2. Загрузка данных остатков
3. Объединение данных с обработкой отсутствующих значений
4. Фильтрация по категории (если указана)

### 2. Приоритизация товаров
1. Сортировка по полю `sort` (если включено)
2. Группировка по заданным колонкам
3. Расчет рейтинга каждой группы

### 3. Создание пулов компенсаторов
1. Поиск товаров с высоким рейтингом (>= min_group_rating)
2. Группировка компенсаторов по категории/полу
3. Приоритизация компенсаторов без остатков

### 4. Компенсация групп
1. Определение групп, требующих компенсации
2. Справедливое распределение компенсаторов
3. Добавление компенсаторов в группы
4. Пересчет рейтинга групп

### 5. Финализация
1. Ограничение размера групп
2. Выявление проблемных товаров
3. Расчет финальной статистики

## Сравнение с оригинальной версией

| Аспект | Оригинальная версия | Улучшенная версия |
|--------|-------------------|------------------|
| SQL запросы | INNER JOIN (неправильно) | LEFT JOIN (правильно) |
| Распределение компенсаторов | Жадное (глобальный набор) | Справедливое (локальные пулы) |
| Логирование | Отсутствует | Детальное |
| Архитектура | Монолитная | Модульная |
| Тестируемость | Низкая | Высокая |
| Отладка | Сложная | Простая |
| Предсказуемость | Низкая | Высокая |

## Производительность

### Оптимизации
1. **Эффективные SQL запросы** - использование индексов и оптимальных JOIN
2. **Пакетная обработка** - минимизация количества запросов к БД
3. **Кэширование** - повторное использование загруженных данных
4. **Ленивые вычисления** - расчет статистики только при необходимости

### Рекомендации по масштабированию
- Для больших объемов данных (>10000 SKU) рекомендуется пакетная обработка
- Использование индексов на колонках `wb_sku`, `gender`, `wb_category`
- Мониторинг использования памяти при работе с большими группами

## Тестирование

### Модульные тесты
```python
def test_grouping_basic():
    """Тест базовой группировки."""
    config = GroupingConfig(
        grouping_columns=['gender'],
        min_group_rating=4.0
    )
    grouper = AdvancedProductGrouper(mock_conn)
    result = grouper.create_advanced_product_groups(['sku1', 'sku2'], config)
    
    assert len(result.groups) > 0
    assert result.statistics['total_groups'] > 0

def test_compensator_distribution():
    """Тест справедливого распределения компенсаторов."""
    # Тест логики распределения
    pass
```

### Интеграционные тесты
```python
def test_full_pipeline():
    """Тест полного пайплайна группировки."""
    # Тест с реальной БД
    pass
```

## Мониторинг и отладка

### Логи
Все операции логируются с указанием уровня важности:
- `[INFO]` - информационные сообщения
- `[WARNING]` - предупреждения
- `[ERROR]` - ошибки

### Метрики
- Количество созданных групп
- Средний размер группы
- Средний рейтинг групп
- Количество использованных компенсаторов
- Время выполнения операций

## Развитие и поддержка

### Планы развития
1. **Машинное обучение** - автоматическая оптимизация параметров группировки
2. **A/B тестирование** - сравнение различных стратегий группировки
3. **Кэширование результатов** - ускорение повторных запросов
4. **API интерфейс** - возможность использования без UI

### Известные ограничения
1. Производительность при очень больших объемах данных (>50000 SKU)
2. Зависимость от качества данных в таблицах Punta и остатков
3. Ограниченная настройка алгоритма компенсации

## Миграция с оригинальной версии

### Шаги миграции
1. Сохранить результаты текущей группировки
2. Установить новые модули
3. Протестировать на небольшом наборе данных
4. Сравнить результаты с оригинальной версией
5. Полная миграция

### Обратная совместимость
Новая версия не является обратно совместимой с оригинальной из-за:
- Изменения в структуре результатов
- Новых параметров конфигурации
- Другого формата логов

## Заключение

Улучшенная версия группировки товаров решает основные проблемы оригинальной реализации и предоставляет более надежный, предсказуемый и масштабируемый функционал. Модульная архитектура облегчает тестирование, отладку и дальнейшее развитие системы.