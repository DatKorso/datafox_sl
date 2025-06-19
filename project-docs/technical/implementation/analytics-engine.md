# 📊 Аналитический движок DataFox SL

> Техническая документация системы генерации аналитических отчетов

## 🎯 Обзор системы

Аналитический движок DataFox SL предназначен для автоматизированной генерации детальных Excel отчетов с кросс-маркетплейс аналитикой, включая сопоставление товаров, анализ продаж и интеграцию изображений.

## 🏗️ Архитектура движка

### Основные компоненты

#### **1. Загрузчик и валидатор отчетов**
```python
# analytic_report_helpers.py - Основной модуль
def load_analytic_report_file(file_path: str) -> Tuple[Optional[pd.DataFrame], Optional[openpyxl.Workbook], str]:
    """
    Загружает и валидирует структуру Excel файла аналитического отчета
    
    Ожидаемая структура:
    - Лист "analytic_report"
    - Заголовки в строке 7
    - Данные начиная с строки 9
    - Обязательная колонка "WB_SKU"
    """
```

#### **2. Система сопоставления товаров**
```python
def map_wb_to_ozon_by_size(db_conn, wb_sku_list: List[str]) -> Dict[str, Dict[int, List[str]]]:
    """
    Сопоставляет товары WB с товарами Ozon по размерам через общие штрихкоды
    
    Алгоритм:
    1. Получение WB товаров с размерами и штрихкодами
    2. Нормализация штрихкодов WB (разделение по ;)
    3. Поиск соответствий в Ozon по штрихкодам
    4. Группировка по размерам
    
    Returns:
        Dict[wb_sku][size] -> [list of oz_skus]
    """
```

#### **3. Агрегатор данных**
```python
def aggregate_stock_data(db_conn, oz_sku_list: List[str]) -> int:
    """Агрегирует остатки по списку Ozon SKU"""

def generate_order_statistics(db_conn, oz_sku_list: List[str], days_back: int = 30) -> Dict[str, int]:
    """
    Генерирует статистику заказов за указанный период
    
    Returns:
        {
            'total_orders': int,
            'total_revenue': int, 
            'avg_order_value': int,
            'orders_last_7_days': int,
            'orders_last_14_days': int
        }
    """
```

### Поток обработки данных

Процесс включает следующие этапы:

1. **Валидация структуры** Excel файла
2. **Извлечение WB_SKU** из данных
3. **Поиск товаров WB** в базе данных
4. **Нормализация штрихкодов** WB
5. **Сопоставление с Ozon** через общие штрихкоды
6. **Группировка по размерам**
7. **Агрегация остатков** и статистики
8. **Получение Punta данных**
9. **Загрузка изображений** WB (опционально)
10. **Обновление Excel файла** с результатами

## 🔧 Техническая реализация

### Алгоритм сопоставления товаров

#### **Этап 1: Нормализация штрихкодов WB**
```sql
-- WB хранит штрихкоды как строку "123456;789012;555444"
WITH split_barcodes AS (
    SELECT 
        p.wb_sku,
        p.wb_size,
        TRIM(barcode_part) as individual_barcode
    FROM wb_products p,
    UNNEST(regexp_split_to_array(p.wb_barcodes, E'[\\s;]+')) AS t(barcode_part)
    WHERE p.wb_sku IN (?) AND barcode_part IS NOT NULL
)
SELECT wb_sku, wb_size, individual_barcode
FROM split_barcodes
WHERE LENGTH(TRIM(individual_barcode)) >= 8
```

#### **Этап 2: Поиск соответствий в Ozon**
```sql
-- Связь через общие штрихкоды
SELECT DISTINCT
    p.oz_sku,
    b.oz_barcode
FROM oz_products p
JOIN oz_barcodes b ON p.oz_product_id = b.oz_product_id
WHERE NULLIF(TRIM(b.oz_barcode), '') IS NOT NULL
```

#### **Этап 3: Группировка по размерам**
```python
def build_size_mapping(matched_df: pd.DataFrame) -> Dict[str, Dict[int, List[str]]]:
    """
    Строит иерархическую структуру:
    wb_sku -> размер -> список oz_sku
    """
    result_map = {}
    
    for _, row in matched_df.iterrows():
        wb_sku = str(row['wb_sku'])
        wb_size = int(float(row['wb_size'])) if pd.notna(row['wb_size']) else None
        oz_sku = str(row['oz_sku'])
        
        if wb_sku not in result_map:
            result_map[wb_sku] = {}
        
        if wb_size is not None:
            if wb_size not in result_map[wb_sku]:
                result_map[wb_sku][wb_size] = []
            if oz_sku not in result_map[wb_sku][wb_size]:
                result_map[wb_sku][wb_size].append(oz_sku)
    
    return result_map
```

### Расчет размерных диапазонов

#### **Определение диапазона размеров**
```python
def calculate_size_range(size_mapping: Dict[int, List[str]]) -> str:
    """
    Вычисляет строку диапазона размеров
    
    Examples:
        {27: ['123'], 28: ['124'], 30: ['125']} -> "27-30"
        {40: ['126']} -> "40"
        {} -> ""
    """
    sizes_with_skus = [size for size, skus in size_mapping.items() if skus]
    
    if not sizes_with_skus:
        return ""
    
    min_size = min(sizes_with_skus)
    max_size = max(sizes_with_skus)
    
    return str(min_size) if min_size == max_size else f"{min_size}-{max_size}"
```

### Статистика заказов

#### **Агрегация данных заказов**
```sql
-- Статистика заказов за период
SELECT 
    COUNT(*) as total_orders,
    SUM(p.oz_actual_price) as total_revenue,
    AVG(p.oz_actual_price) as avg_order_value,
    COUNT(CASE WHEN o.oz_accepted_date >= ? THEN 1 END) as orders_last_7_days,
    COUNT(CASE WHEN o.oz_accepted_date >= ? THEN 1 END) as orders_last_14_days
FROM oz_orders o
JOIN oz_products p ON o.oz_sku = p.oz_sku
WHERE o.oz_sku IN (?)
    AND o.oz_accepted_date >= ?
    AND o.order_status NOT IN ('cancelled', 'returned')
```

### Интеграция изображений

#### **Получение изображений Wildberries**
```python
def get_wb_image_url(wb_sku: str) -> str:
    """
    Генерирует URL изображения товара WB по алгоритму:
    
    1. Преобразование SKU в корзину и том:
       - Корзина = SKU // 100000
       - Том = (SKU // 1000) % 1000
    
    2. Формирование URL:
       https://basket-{корзина:02d}.wb.ru/vol{том}/part{SKU//1000}/{SKU}/images/big/1.jpg
    """
    try:
        sku_int = int(wb_sku)
        basket = sku_int // 100000
        vol = (sku_int // 1000) % 1000
        part = sku_int // 1000
        
        url = f"https://basket-{basket:02d}.wb.ru/vol{vol}/part{part}/{sku_int}/images/big/1.jpg"
        return url
    except (ValueError, TypeError):
        return ""
```

#### **Загрузка и вставка изображений**
```python
def download_wb_image(wb_sku: str, timeout: int = 30) -> Optional[BytesIO]:
    """
    Загружает изображение товара WB с обработкой ошибок
    """
    try:
        url = get_wb_image_url(wb_sku)
        if not url:
            return None
            
        response = requests.get(url, timeout=timeout, 
                              headers={'User-Agent': 'Mozilla/5.0'})
        
        if response.status_code == 200:
            img_data = BytesIO(response.content)
            # Валидация изображения
            PILImage.open(img_data).verify()
            img_data.seek(0)
            return img_data
            
    except Exception as e:
        logging.warning(f"Ошибка загрузки изображения для WB SKU {wb_sku}: {e}")
    
    return None

def insert_wb_image_to_cell(ws, row_num: int, col_num: int, wb_sku: str, 
                           cell_width: float = 64, cell_height: float = 64) -> Tuple[bool, Optional[str]]:
    """
    Вставляет изображение WB товара в Excel ячейку
    """
    try:
        img_data = download_wb_image(wb_sku)
        if not img_data:
            return False, f"Не удалось загрузить изображение для WB SKU {wb_sku}"
        
        # Создание временного файла
        with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as tmp_file:
            tmp_file.write(img_data.getvalue())
            tmp_path = tmp_file.name
        
        try:
            # Вставка изображения в Excel
            img = Image(tmp_path)
            img.width = cell_width
            img.height = cell_height
            
            cell = ws.cell(row=row_num, column=col_num)
            img.anchor = cell.coordinate
            ws.add_image(img)
            
            return True, None
            
        finally:
            os.unlink(tmp_path)  # Удаление временного файла
            
    except Exception as e:
        return False, f"Ошибка вставки изображения: {str(e)}"
```

## 📊 Интеграция с Punta

### Динамические колонки

#### **Получение схемы Punta**
```python
def get_dynamic_punta_columns(db_conn) -> List[str]:
    """
    Динамически определяет доступные колонки в таблице punta_table
    
    Возвращает список колонок исключая системные (_source_file, _import_date)
    """
    try:
        # Получение схемы таблицы
        schema_query = "DESCRIBE punta_table"
        schema_df = db_conn.execute(schema_query).fetchdf()
        
        # Фильтрация системных колонок
        system_columns = ['_source_file', '_import_date']
        available_columns = [
            col for col in schema_df['column_name'].tolist()
            if col not in system_columns
        ]
        
        return available_columns
        
    except Exception as e:
        st.warning(f"Ошибка получения колонок Punta: {e}")
        return []
```

#### **Получение данных Punta**
```python
def get_punta_data(db_conn, wb_sku_list: List[str]) -> Dict[str, Dict[str, str]]:
    """
    Получает данные из таблицы punta_table для списка WB SKU
    
    Returns:
        Dict[wb_sku] -> Dict[column_name] -> value
    """
    if not wb_sku_list:
        return {}
    
    try:
        # Попытка поиска по разным полям
        wb_skus_str = [str(sku) for sku in wb_sku_list]
        
        # Получение всех колонок Punta
        available_columns = get_dynamic_punta_columns(db_conn)
        if not available_columns:
            return {}
        
        columns_str = ', '.join([f'"{col}"' for col in available_columns])
        
        # Поиск соответствий
        query = f"""
        SELECT {columns_str}
        FROM punta_table
        WHERE vendor_code IN ({', '.join(['?'] * len(wb_skus_str))})
        """
        
        punta_df = db_conn.execute(query, wb_skus_str).fetchdf()
        
        # Преобразование в словарь
        result = {}
        for _, row in punta_df.iterrows():
            vendor_code = str(row.get('vendor_code', ''))
            if vendor_code in wb_skus_str:
                result[vendor_code] = {
                    col: str(row.get(col, '')) 
                    for col in available_columns
                }
        
        return result
        
    except Exception as e:
        st.error(f"Ошибка получения данных Punta: {e}")
        return {}
```

## 🔄 Обновление Excel файлов

### Сохранение структуры файла

#### **Основная функция обновления**
```python
def update_analytic_report(file_path: str, wb_sku_data: Dict[str, Dict], 
                          include_images: bool = False) -> Tuple[bool, str]:
    """
    Обновляет Excel файл аналитического отчета с сохранением исходной структуры
    
    Процесс:
    1. Создание резервной копии
    2. Загрузка workbook
    3. Поиск соответствующих строк по WB_SKU
    4. Обновление данных в найденных ячейках
    5. Добавление изображений (опционально)
    6. Сохранение файла
    """
    try:
        # Создание резервной копии
        backup_path = create_backup_file(file_path)
        st.info(f"Создана резервная копия: {backup_path}")
        
        # Загрузка workbook
        wb = load_workbook(file_path)
        ws = wb["analytic_report"]
        
        # Поиск колонки WB_SKU (обычно строка 7)
        wb_sku_col = None
        for col_idx in range(1, ws.max_column + 1):
            cell_value = ws.cell(row=7, column=col_idx).value
            if cell_value and "WB_SKU" in str(cell_value):
                wb_sku_col = col_idx
                break
        
        if not wb_sku_col:
            return False, "Колонка WB_SKU не найдена в строке 7"
        
        # Обновление данных начиная с строки 9
        updated_count = 0
        for row_idx in range(9, ws.max_row + 1):
            wb_sku_cell = ws.cell(row=row_idx, column=wb_sku_col)
            wb_sku = str(wb_sku_cell.value or "").strip()
            
            if wb_sku in wb_sku_data:
                row_data = wb_sku_data[wb_sku]
                
                # Обновление данных по колонкам
                for field_name, col_mapping in COLUMN_MAPPINGS.items():
                    if field_name in row_data:
                        col_idx = col_mapping.get('column_index')
                        if col_idx:
                            ws.cell(row=row_idx, column=col_idx).value = row_data[field_name]
                
                # Добавление изображения
                if include_images and 'image_column' in COLUMN_MAPPINGS:
                    img_col = COLUMN_MAPPINGS['image_column']['column_index']
                    success, error = insert_wb_image_to_cell(ws, row_idx, img_col, wb_sku)
                    if not success and error:
                        st.warning(error)
                
                updated_count += 1
        
        # Сохранение файла
        wb.save(file_path)
        return True, f"Обновлено {updated_count} строк в файле"
        
    except Exception as e:
        return False, f"Ошибка обновления файла: {str(e)}"
```

### Конфигурация колонок

#### **Маппинг полей на колонки Excel**
```python
COLUMN_MAPPINGS = {
    'size_range': {'column_index': 5, 'header': 'Размерный ряд'},
    'total_stock': {'column_index': 6, 'header': 'Общий остаток'},
    'total_orders': {'column_index': 7, 'header': 'Заказы (30 дней)'},
    'total_revenue': {'column_index': 8, 'header': 'Выручка (30 дней)'},
    'orders_last_7_days': {'column_index': 9, 'header': 'Заказы (7 дней)'},
    'orders_last_14_days': {'column_index': 10, 'header': 'Заказы (14 дней)'},
    'image_column': {'column_index': 11, 'header': 'Изображение'},
    # Динамические колонки Punta добавляются программно
}
```

## 📈 Метрики и мониторинг

### Сбор статистики обработки

```python
def collect_processing_metrics(wb_sku_data: Dict) -> Dict:
    """
    Собирает метрики обработки аналитического отчета
    """
    total_wb_skus = len(wb_sku_data)
    successful_mappings = len([
        data for data in wb_sku_data.values()
        if data.get('mapped_oz_skus')
    ])
    
    total_oz_skus = sum(
        len(data.get('mapped_oz_skus', []))
        for data in wb_sku_data.values()
    )
    
    return {
        'total_wb_skus_processed': total_wb_skus,
        'successful_mappings': successful_mappings,
        'mapping_success_rate': (successful_mappings / total_wb_skus * 100) if total_wb_skus > 0 else 0,
        'total_oz_skus_found': total_oz_skus,
        'avg_oz_skus_per_wb': total_oz_skus / successful_mappings if successful_mappings > 0 else 0
    }
```

### Производительность системы

```python
def monitor_performance():
    """
    Мониторинг производительности аналитического движка
    """
    metrics = {
        'barcode_normalization_time': 0,  # время нормализации штрихкодов
        'matching_algorithm_time': 0,     # время алгоритма сопоставления
        'data_aggregation_time': 0,       # время агрегации данных
        'excel_update_time': 0,           # время обновления Excel
        'image_download_time': 0,         # время загрузки изображений
        'total_processing_time': 0        # общее время обработки
    }
    
    return metrics
```

## 🚨 Обработка ошибок

### Типы ошибок

#### **1. Ошибки валидации файлов**
```python
class ReportValidationError(Exception):
    """Ошибка валидации структуры отчета"""
    pass

class MissingColumnError(Exception):
    """Отсутствует обязательная колонка"""
    pass
```

#### **2. Ошибки обработки данных**
```python
class DataMappingError(Exception):
    """Ошибка сопоставления данных"""
    pass

class AggregationError(Exception):
    """Ошибка агрегации данных"""
    pass
```

#### **3. Ошибки интеграции**
```python
class ImageDownloadError(Exception):
    """Ошибка загрузки изображений"""
    pass

class ExcelUpdateError(Exception):
    """Ошибка обновления Excel файла"""
    pass
```

---

## 📝 Метаданные

**Модуль**: `utils/analytic_report_helpers.py`  
**Размер**: 941 строк кода  
**Последнее обновление**: 2024-12-19  
**Версия**: 1.3.0  
**Статус**: Стабильный  

**Связанные документы**:
- [Архитектура системы](../architecture-overview.md)
- [Database Utils API](../api/database-utils.md)
- [Поисковые алгоритмы](search-algorithms.md)

*Аналитический движок обеспечивает полную автоматизацию создания детальных отчетов с кросс-маркетплейс данными.* 