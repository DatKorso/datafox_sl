# Руководство по диагностике проблем с WB рекомендациями

## Описание проблемы

Пользователь сообщил о зависании страницы "16_🎯_Рекомендации_WB.py" со статусом "Running..." после вставки WB SKU. Подозревается бесконечный цикл или зависание SQL запросов.

## Реализованные решения

### 1. Улучшенное логгирование

#### Страница Streamlit (`pages/16_🎯_Рекомендации_WB.py`)
- ✅ Добавлено детальное логгирование всех операций
- ✅ Логи записываются в файл `wb_recommendations.log`
- ✅ Добавлен Debug режим с информацией о состоянии приложения
- ✅ Логи доступны в сайдбаре для просмотра
- ✅ Добавлены spinner'ы для долгих операций

#### Модуль WB рекомендаций (`utils/wb_recommendations.py`)
- ✅ Добавлено логгирование всех SQL запросов с измерением времени выполнения
- ✅ Логгирование каждого этапа обработки
- ✅ Детальная диагностика ошибок
- ✅ Упрощен проблемный SQL запрос в `get_statistics()`

### 2. Оптимизация SQL запросов

#### Метод `get_statistics()`
- ✅ Упрощен сложный JOIN запрос, который мог вызывать зависание
- ✅ Добавлено измерение времени выполнения каждого запроса
- ✅ Добавлена обработка ошибок с возвратом значений по умолчанию

#### Методы получения данных
- ✅ Добавлено логгирование времени выполнения SQL запросов
- ✅ Логгирование прогресса обработки больших наборов данных
- ✅ Улучшена обработка ошибок

#### Исправление обогащения Punta данными
- ✅ **ИСПРАВЛЕНО**: Метод `_enrich_with_punta_data()` теперь использует `wb_sku` вместо `vendor_code`
- ✅ Исправлена ошибка "Referenced column 'vendor_code' not found" в таблице `punta_table`
- ✅ Правильная логика: `punta_table` связывается по `wb_sku`, а не по `vendor_code`

### 3. Тест-скрипт для диагностики

Создан скрипт `test_wb_recommendations_debug.py` для:
- ✅ Проверки подключения к базе данных
- ✅ Тестирования базовых SQL запросов
- ✅ Проверки работы WB рекомендаций
- ✅ Детального логгирования всех операций

## Инструкции по диагностике

### Шаг 1: Включение Debug режима
1. Откройте страницу "16_🎯_Рекомендации_WB.py"
2. Поставьте галочку "🐛 Debug режим" в верхней части страницы
3. Проверьте информацию о состоянии приложения

### Шаг 2: Запуск тест-скрипта
```bash
# Из корневой директории проекта
python test_wb_recommendations_debug.py
```

Скрипт проверит:
- Подключение к базе данных
- Базовые SQL запросы
- Работу WB рекомендаций процессора
- Создаст детальные логи

### Шаг 3: Просмотр логов
После запуска проверьте следующие файлы логов:
- `wb_recommendations.log` - основные логи приложения
- `wb_recommendations_debug.log` - логи тест-скрипта
- В сайдбаре Streamlit: "📝 Логи" → последние 10 строк

### Шаг 4: Анализ логов
Ищите в логах:
- Время выполнения SQL запросов (> 5 секунд - подозрительно)
- Ошибки подключения к базе данных
- Исключения в обработке данных
- Зависания на конкретных WB SKU

## Возможные причины зависания

### 1. Медленные SQL запросы
**Симптомы:**
- Логи показывают долгое выполнение SQL запросов
- Особенно JOIN операции с большими таблицами

**Решение:**
- Проверить индексы в базе данных
- Оптимизировать запросы
- Добавить LIMIT для больших выборок

### 2. Проблемы с CrossMarketplaceLinker
**Симптомы:**
- Зависание на этапе обогащения данных
- Ошибки связывания WB и Ozon товаров

**Решение:**
- Проверить соединение с интернетом
- Проверить работу CrossMarketplaceLinker отдельно
- Добавить таймауты для внешних запросов

### 3. Большие объемы данных
**Симптомы:**
- Зависание при обработке большого количества WB SKU
- Медленная обработка кандидатов

**Решение:**
- Уменьшить количество обрабатываемых SKU
- Добавить пагинацию
- Оптимизировать алгоритмы

### 4. Проблемы с памятью
**Симптомы:**
- Зависание при создании больших DataFrame
- Медленная работа Streamlit

**Решение:**
- Проверить использование памяти
- Оптимизировать обработку данных
- Добавить очистку кэша

## Рекомендации по устранению

### Для пользователя:
1. **Включите Debug режим** - это поможет увидеть, на каком этапе происходит зависание
2. **Запустите тест-скрипт** - он поможет выявить проблему
3. **Проверьте логи** - они покажут точное место зависания
4. **Начните с малого** - тестируйте с 1-2 WB SKU, а не сразу с большим списком

### Для разработчика:
1. **Добавить таймауты** для всех SQL запросов
2. **Оптимизировать JOIN операции** с большими таблицами
3. **Добавить пагинацию** для больших наборов данных
4. **Внедрить кэширование** для часто запрашиваемых данных

## Файлы, затронутые изменениями

1. **pages/16_🎯_Рекомендации_WB.py** - добавлено детальное логгирование
2. **utils/wb_recommendations.py** - оптимизированы SQL запросы, добавлено логгирование
3. **test_wb_recommendations_debug.py** - новый тест-скрипт для диагностики

## Следующие шаги

1. Запустить тест-скрипт для выявления проблемы
2. Проанализировать логи
3. Оптимизировать выявленные узкие места
4. Добавить мониторинг производительности
5. Внедрить предупреждения о долгих операциях

---

*Дата создания: 2024-01-XX*  
*Версия: 1.0*  
*Автор: DataFox SL Assistant* 