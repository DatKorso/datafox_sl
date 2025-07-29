# 🛠️ Оптимизация памяти для Rich Content OZ

> Решение проблем с WebSocket ошибками при обработке больших объемов данных

## 🚨 Проблема

При обработке больших объемов товаров (~8000) на странице "11_Rich_Контент_OZ" возникают WebSocket ошибки:

```
asyncio.exceptions.CancelledError
RuntimeError: no running event loop
```

**Причина**: Streamlit пытается передать слишком много данных через WebSocket соединение, что приводит к:
- Переполнению памяти браузера
- Разрыву WebSocket соединения
- Зависанию интерфейса

## ✅ Решения

### 1. Потоковая обработка с промежуточным сохранением

```python
def process_batch_streaming(self, oz_vendor_codes: List[str], auto_save: bool = True) -> BatchResult:
    """
    Потоков��я обработка с автоматическим сохранением в БД
    Не накапливает данные в памяти браузера
    """
    total_items = len(oz_vendor_codes)
    chunk_size = 100  # Обрабатываем по 100 товаров
    
    stats = {'successful': 0, 'errors': 0}
    
    for i in range(0, total_items, chunk_size):
        chunk = oz_vendor_codes[i:i + chunk_size]
        
        # Обрабатываем чанк
        chunk_results = self._process_chunk(chunk)
        
        # Сразу сохраняем в БД
        if auto_save:
            for result in chunk_results:
                if result.success:
                    self.save_rich_content_to_database(result)
                    stats['successful'] += 1
                else:
                    stats['errors'] += 1
        
        # Очищаем память
        del chunk_results
        
        # Обновляем прогресс
        yield i + len(chunk), total_items, stats
```

### 2. Легковесные результаты

```python
@dataclass
class LightweightResult:
    """Легковесная версия результата без тяжелых данных"""
    oz_vendor_code: str
    status: ProcessingStatus
    success: bool
    processing_time: float
    error_message: Optional[str] = None
    recommendations_count: int = 0
    # Убираем тяжелые поля: recommendations, rich_content_json
```

### 3. Прямое сохранение в БД без session_state

```python
def process_large_batch_direct_save(self, oz_vendor_codes: List[str]) -> Dict[str, Any]:
    """
    Обработка больших пакетов с прямым сохранением в БД
    Возвращает только статистику, не данные
    """
    stats = {
        'total': len(oz_vendor_codes),
        'successful': 0,
        'errors': 0,
        'start_time': time.time()
    }
    
    for vendor_code in oz_vendor_codes:
        try:
            result = self.process_single_product(vendor_code)
            
            if result.success:
                # Сразу сохраняем в БД
                self.save_rich_content_to_database(result)
                stats['successful'] += 1
            else:
                stats['errors'] += 1
                
        except Exception as e:
            stats['errors'] += 1
            logger.error(f"Ошибка обработки {vendor_code}: {e}")
    
    stats['processing_time'] = time.time() - stats['start_time']
    return stats
```

## 🔧 Реализованные исправления

### 1. Защита от WebSocket ошибок в UI

```python
# В pages/11_🚧_Rich_Контент_OZ.py
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
```

### 2. Автоматическое создание легковесных результатов

```python
# Для больших пакетов (>1000 товаров)
if len(batch_result.processed_items) > 1000:
    st.warning("⚠️ **Большой пакет обнаружен** - создаем легковесную версию")
    
    lightweight_items = []
    for item in batch_result.processed_items:
        lightweight_item = LightweightResult(
            oz_vendor_code=item.oz_vendor_code,
            status=item.status,
            success=item.success,
            processing_time=item.processing_time,
            error_message=item.error_message,
            recommendations_count=len(item.recommendations)
        )
        lightweight_items.append(lightweight_item)
    
    # Сохраняем легковесную версию
    st.session_state.last_batch_result = create_lightweight_batch_result(lightweight_items)
```

### 3. Альтернативный экспорт из БД

```python
def export_from_database_directly():
    """Экспорт Rich Content напрямую из БД без session_state"""
    query = """
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
    
    results = conn.execute(query).fetchall()
    
    if results:
        df = pd.DataFrame(results, columns=['oz_vendor_code', 'oz_sku', 'rich_content'])
        csv_content = df.to_csv(index=False).encode('utf-8')
        
        st.download_button(
            label="💾 Скачать из БД",
            data=csv_content,
            file_name=f"rich_content_from_db_{int(time.time())}.csv",
            mime="text/csv"
        )
```

## 📊 Рекомендации по размерам пакетов

| Размер пакета | Рекомендуемый подход | Ожидаемое поведение |
|---------------|---------------------|---------------------|
| **< 100 товаров** | Стандартная обработка | Полные результаты в UI |
| **100-1000 товаров** | Оптимизированная обработка | Полные результаты + предупреждения |
| **1000-5000 товаров** | Легковесные результаты | Статистика + экспорт из БД |
| **> 5000 товаров** | Потоковая обработка | Только прогресс + финальная статистика |

## 🚀 Новые возможности

### 1. Режим "Только сохранение"

```python
# Новый режим для больших объемов
processing_mode = st.radio(
    "Режим обработки:",
    ["standard", "optimized", "save_only"],
    format_func=lambda x: {
        "standard": "🐌 Стандартная обработка",
        "optimized": "⚡ Оптимизированная обработка", 
        "save_only": "💾 Только сохранение (для больших объемов)"
    }[x]
)
```

### 2. Мониторинг памяти

```python
def check_memory_usage():
    """Проверка использования памяти"""
    import psutil
    process = psutil.Process()
    memory_mb = process.memory_info().rss / 1024 / 1024
    
    if memory_mb > 1000:  # Больше 1GB
        st.warning(f"⚠️ Высокое использование памяти: {memory_mb:.1f} МБ")
        return True
    return False
```

### 3. Автоматическая очистка памяти

```python
def auto_cleanup_large_results():
    """Автоматическая очистка больших результатов"""
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
            st.info("🧹 Автоматическая очистка памяти выполнена")
```

## 🔍 Диагностика проблем

### Признаки проблем с памятью:
- WebSocket ошибки `CancelledError`
- Зависание страницы на "Running..."
- Медленная работа браузера
- Ошибки `RuntimeError: no running event loop`

### Решения:
1. **Уменьшить размер пакета** - обрабатывать по частям
2. **Использовать режим "save_only"** - для больших объемов
3. **Экспортировать из БД** - вместо session_state
4. **Очистить память** - кнопка очистки результатов
5. **Обновить страницу** - F5 для сброса состояния

## 📝 Чек-лист для больших объемов

- [ ] Размер пакета > 1000? → Использовать легковесные результаты
- [ ] Размер пакета > 5000? → Использовать режим "save_only"
- [ ] WebSocket ошибки? → Экспорт из БД + очистка памяти
- [ ] Медленная работа? → Проверить использование памяти
- [ ] Зависание UI? → Обновить страницу + уменьшить пакет

---

**Статус**: ✅ Реализовано  
**Дата**: 2024-12-19  
**Версия**: 1.0.0  
**Тестировано на**: 8000 товаров  