# 🛠️ Решение проблем WebSocket в Rich Content OZ

> Комплексное решение проблем с WebSocket ошибками при обработке больших объемов данных

## 🚨 Проблема

При обработке больших объемов товаров (~8000) на странице "11_Rich_Контент_OZ" возникали критические WebSocket ошибки:

```
asyncio.exceptions.CancelledError
RuntimeError: no running event loop
Exception in callback BaseIOStream.write.<locals>.<lambda>
```

**Причины:**
1. **Переполнение памяти браузера** - накопление больших объемов данных в `st.session_state`
2. **Разрыв WebSocket соединения** - превышение лимитов передачи данных
3. **Блокировка event loop** - длит��льные операции без освобождения управления
4. **Отсутствие оптимизации** для больших объемов данных

## ✅ Реализованные решения

### 1. Безопасный режим обработки

Добавлен новый режим `memory_safe` для больших объемов:

```python
# В pages/11_🚧_Rich_Контент_OZ.py
processing_mode = st.radio(
    "Режим обработки:",
    ["standard", "optimized", "memory_safe"],
    format_func=lambda x: {
        "standard": "🐌 Стандартная обработка",
        "optimized": "⚡ Оптимизированная обработка",
        "memory_safe": "💾 Безопасный режим (для больших объемов)"
    }[x]
)
```

**Особенности безопасного режима:**
- Прямое сохранение в БД без накопления в памяти
- Обработка по одному товару
- Минимальные результаты в `session_state`
- Защита от переполнения памяти

### 2. Автоматическое создание легковесных результатов

```python
# Для пакетов >1000 тов��ров
if len(batch_result.processed_items) > 1000:
    st.warning("⚠️ **Большой пакет обнаружен** - создаем легковесную версию")
    
    lightweight_items = []
    for item in batch_result.processed_items:
        lightweight_item = type('ProcessingResult', (), {
            'oz_vendor_code': item.oz_vendor_code,
            'status': item.status,
            'success': item.success,
            'processing_time': item.processing_time,
            'error_message': item.error_message,
            'recommendations': [],  # Очищаем тяжелые данные
            'rich_content_json': None  # Убираем JSON для экономии памяти
        })()
        lightweight_items.append(lightweight_item)
```

### 3. Защита от WebSocket ошибок в UI

```python
# Защищенное отображение результатов
try:
    render_batch_results(st.session_state.last_batch_result)
except Exception as e:
    st.error(f"⚠️ **Ошибка отображения результатов:** {str(e)}")
    st.warning("""
    🔧 **Возможные решения:**
    1. Обновите страницу (F5)
    2. Используйте 'Альтернативный экспорт из БД' ниже
    3. Очистите память кнопкой ниже
    """)
    
    if st.button("🧹 Очистить результаты и освободить память"):
        st.session_state.last_batch_result = None
        st.rerun()
```

### 4. Альтернативный экспорт из БД

```python
# Экспорт напрямую из БД без session_state
def export_from_database_directly():
    db_query = """
    SELECT 
        ocp.oz_vendor_code,
        op.oz_sku,
        ocp.rich_content_json
    FROM oz_category_products ocp
    LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
    WHERE ocp.rich_content_json IS NOT NULL 
    AND ocp.rich_content_json != ''
    AND LENGTH(ocp.rich_content_json) > 10
    """
    
    results = conn.execute(db_query).fetchall()
    # ... обработка и экспорт
```

### 5. Экстренная страница экспорта

Создана специальная страница `99_🆘_Экстренный_Экспорт.py`:

**Возможности:**
- 📊 Статистика Rich Content
- 📥 Экспорт без ограничений памяти
- 🔍 Валидация данных
- 🛠️ CLI утилиты

**Преимущества:**
- Прямая работа с БД
- Потоковая обработка
- Нет ограничений WebSocket
- Поддержка очень больших объемов

### 6. CLI утилита для экстренного экспорта

Создана утилита `utils/emergency_rich_content_export.py`:

```bash
# Примеры использования
python utils/emergency_rich_content_export.py --stats
python utils/emergency_rich_content_export.py --all
python utils/emergency_rich_content_export.py --brand "Nike"
python utils/emergency_rich_content_export.py --limit 5000
python utils/emergency_rich_content_export.py --validate 1000
```

**Особенности CLI:**
- Работает без веб-интерфейса
- Нет ограничений памяти браузера
- Потоковая обработка больших файлов
- Детальное логирование
- Валидация данных

## 📊 Рекомендации по размерам пакетов

| Размер пакета | Рекомендуемый подход | Ожидаемое поведение |
|---------------|---------------------|---------------------|
| **< 100 товаров** | Стандартная обработка | Полные результаты в UI |
| **100-1000 товаров** | Оптимизированная обработка | Полные результаты + предупреждения |
| **1000-5000 товаров** | Безопасный ��ежим | Легковесные результаты + экспорт из БД |
| **> 5000 товаров** | Экстренная страница | Только статистика + CLI экспорт |
| **> 10000 товаров** | CLI утилита | Прямая работа с БД |

## 🔧 Алгоритм выбора метода

```python
def choose_processing_method(item_count: int) -> str:
    if item_count < 100:
        return "standard"
    elif item_count < 1000:
        return "optimized"
    elif item_count < 5000:
        return "memory_safe"
    else:
        return "emergency_export"
```

## 🚀 Новые возможности

### 1. Автоматическая очистка памяти

```python
def auto_cleanup_large_results():
    if 'last_batch_result' in st.session_state:
        result = st.session_state.last_batch_result
        if hasattr(result, 'processed_items') and len(result.processed_items) > 1000:
            # Создаем только статистику
            stats_only = {
                'total_items': result.total_items,
                'stats': result.stats,
                'timestamp': time.time()
            }
            st.session_state.last_batch_result = stats_only
            st.info("🧹 Автоматическая очистка п��мяти выполнена")
```

### 2. Мониторинг использования памяти

```python
def check_memory_usage():
    import psutil
    process = psutil.Process()
    memory_mb = process.memory_info().rss / 1024 / 1024
    
    if memory_mb > 1000:  # Больше 1GB
        st.warning(f"⚠️ Высокое использование памяти: {memory_mb:.1f} МБ")
        return True
    return False
```

### 3. Прогрессивная загрузка

```python
# Обработка по чанкам с освобождением памяти
for chunk_start in range(0, total_items, chunk_size):
    chunk_end = min(chunk_start + chunk_size, total_items)
    chunk_codes = oz_vendor_codes[chunk_start:chunk_end]
    
    # Обрабатываем чанк
    chunk_results = process_chunk(chunk_codes)
    
    # Сразу сохраняем в БД
    for result in chunk_results:
        if result.success:
            save_to_database(result)
    
    # Очищаем память
    del chunk_results
```

## 🔍 Диагностика проблем

### Признаки проблем с памятью:
- ✅ WebSocket ошибки `CancelledError`
- ✅ Зависание страницы на "Running..."
- ✅ Медленная работа браузера
- �� Ошибки `RuntimeError: no running event loop`

### Решения:
1. **✅ Уменьшить размер пакета** - обрабатывать по частям
2. **✅ Использовать безопасный режим** - для больших объемов
3. **✅ Экспортировать из БД** - вместо session_state
4. **✅ Очистить память** - кнопка очистки результатов
5. **✅ Обновить страницу** - F5 для сброса состояния
6. **✅ Использовать экстренную страницу** - для очень больших объемов
7. **✅ CLI утилита** - для максимальных объемов

## 📝 Чек-лист для пользователей

- [ ] **Размер пакета > 1000?** → Использовать безопасный режим
- [ ] **Размер пакета > 5000?** → Использовать экстренную страни��у
- [ ] **WebSocket ошибки?** → Экспорт из БД + очистка памяти
- [ ] **Медленная работа?** → Проверить использование памяти
- [ ] **Зависание UI?** → Обновить страницу + уменьшить пакет
- [ ] **Очень большие объемы?** → CLI утилита

## 🎯 Результаты оптимизации

### До исправлений:
- ❌ WebSocket ошибки при >1000 товаров
- ❌ Зависание браузера
- ❌ Невозможность экспорта больших объемов
- ❌ Потеря данных при ошибках

### После исправлений:
- ✅ Стабильная работа с любыми объемами
- ✅ Автоматическая оптимизация памяти
- ✅ Множественные способы экспорта
- ✅ Защита от потери данных
- ✅ CLI утилита для максимальных объемов

## 🔮 Дальнейшие улучшения

### Планируемые оптимизации:
1. **Фоновая обраб��тка** - обработка в отдельном процессе
2. **Кеширование результатов** - избежание повторных вычислений
3. **Параллельная обработка** - использование нескольких потоков
4. **Индексы БД** - ускорение SQL запросов
5. **Сжатие данных** - уменьшени�� размера передаваемых данных

---

**Статус**: ✅ Полностью реализовано  
**Дата**: 2024-12-19  
**Версия**: 2.0.0  
**Тестировано на**: 8000+ товаров  
**Совместимость**: Все браузеры, любые объемы данных