# 🔍 Поисковые алгоритмы DataFox SL

> Техническая документация алгоритмов кросс-маркетплейс поиска и сопоставления

## 🎯 Обзор системы поиска

Система поиска DataFox SL обеспечивает связывание товаров между маркетплейсами Wildberries и Ozon через несколько алгоритмов сопоставления, используя штрихкоды, артикулы и метаданные товаров.

## 🏗️ Архитектура поиска

### Ключевые компоненты

#### **1. Нормализация штрихкодов**
```python
# db_search_helpers.py - Основной модуль поиска
def get_normalized_wb_barcodes(con: duckdb.DuckDBPyConnection, wb_skus: list[str] = None) -> pd.DataFrame:
    """
    Нормализует штрихкоды Wildberries из формата "123456;789012;555444"
    в отдельные строки для каждого штрихкода
    
    Returns:
        DataFrame с колонками ['wb_sku', 'individual_barcode_wb']
    """
```

#### **2. Извлечение идентификаторов Ozon**
```python
def get_ozon_barcodes_and_identifiers(
    con: duckdb.DuckDBPyConnection,
    oz_skus: list[str] = None,
    oz_vendor_codes: list[str] = None,
    oz_product_ids: list[str] = None
) -> pd.DataFrame:
    """
    Получает идентификаторы Ozon и связанные штрихкоды
    
    Returns:
        DataFrame с колонками ['oz_barcode', 'oz_sku', 'oz_vendor_code', 'oz_product_id']
    """
```

#### **3. Кросс-маркетплейс сопоставление**
```python
def find_cross_marketplace_matches(
    con: duckdb.DuckDBPyConnection,
    search_criterion: str, 
    search_values: list[str],
    selected_fields_map: dict
) -> pd.DataFrame:
    """
    Находит соответствия между Ozon и Wildberries на основе общих штрихкодов
    
    Поддерживаемые критерии поиска:
    - 'wb_sku': поиск по WB SKU
    - 'oz_sku': поиск по Ozon SKU  
    - 'oz_vendor_code': поиск по артикулу Ozon
    - 'barcode': прямой поиск по штрихкоду
    """
```

### Алгоритм поиска

Система использует многоступенчатый процесс:

1. **Определение типа критерия** поиска
2. **Нормализация данных** в зависимости от источника
3. **Сопоставление через общие штрихкоды**
4. **Агрегация и фильтрация результатов**

## 🔧 Техническая реализация

### Нормализация штрихкодов Wildberries

#### **Алгоритм разделения штрихкодов**
```sql
-- WB хранит множественные штрихкоды как "123456;789012;555444"
WITH split_barcodes AS (
    SELECT 
        p.wb_sku,
        UNNEST(string_split(p.wb_barcodes, ';')) AS individual_barcode_wb
    FROM wb_products p
    WHERE NULLIF(TRIM(p.wb_barcodes), '') IS NOT NULL
        AND p.wb_sku IN (?)
)
SELECT 
    wb_sku,
    TRIM(individual_barcode_wb) AS individual_barcode_wb
FROM split_barcodes
WHERE NULLIF(TRIM(individual_barcode_wb), '') IS NOT NULL
```

#### **Валидация штрихкодов**
```python
def validate_barcode(barcode: str) -> bool:
    """
    Валидирует штрихкод по базовым критериям:
    - Минимальная длина 8 символов
    - Только цифры
    - Не пустая строка после trim
    """
    if not barcode or not isinstance(barcode, str):
        return False
    
    clean_barcode = barcode.strip()
    
    return (
        len(clean_barcode) >= 8 and
        clean_barcode.isdigit() and
        clean_barcode != "0" * len(clean_barcode)  # Исключаем нулевые штрихкоды
    )
```

### Извлечение данных Ozon

#### **Получение штрихкодов через product_id**
```sql
-- Связь продуктов Ozon с их штрихкодами
SELECT DISTINCT
    b.oz_barcode,
    p.oz_sku, 
    p.oz_vendor_code, 
    p.oz_product_id
FROM oz_barcodes b
LEFT JOIN oz_products p ON b.oz_product_id = p.oz_product_id
WHERE p.oz_sku IN (?)  -- Фильтрация по критериям поиска
```

#### **Множественные критерии поиска**
```python
def build_ozon_query(oz_skus=None, oz_vendor_codes=None, oz_product_ids=None):
    """
    Динамически строит запрос в зависимости от переданных критериев
    """
    base_query = """
    SELECT DISTINCT
        b.oz_barcode,
        p.oz_sku, 
        p.oz_vendor_code AS product_oz_vendor_code, 
        p.oz_product_id AS product_oz_product_id
    FROM oz_barcodes b
    LEFT JOIN oz_products p ON b.oz_product_id = p.oz_product_id
    """
    
    where_clauses = []
    params = []
    
    if oz_skus:
        skus_for_query = [s for s in oz_skus if str(s).strip().isdigit()]
        if skus_for_query:
            where_clauses.append(f"p.oz_sku IN ({', '.join(['?'] * len(skus_for_query))})")
            params.extend(skus_for_query)
    
    elif oz_vendor_codes:
        vendor_codes_for_query = [str(vc) for vc in oz_vendor_codes if str(vc).strip()]
        if vendor_codes_for_query:
            where_clauses.append(f"p.oz_vendor_code IN ({', '.join(['?'] * len(vendor_codes_for_query))})")
            params.extend(vendor_codes_for_query)
    
    elif oz_product_ids:
        product_ids_for_query = [int(pid) for pid in oz_product_ids if str(pid).strip().isdigit()]
        if product_ids_for_query:
            where_clauses.append(f"p.oz_product_id IN ({', '.join(['?'] * len(product_ids_for_query))})")
            params.extend(product_ids_for_query)
    
    if where_clauses:
        base_query += " WHERE " + " AND ".join(where_clauses)
    
    return base_query, params
```

### Кросс-маркетплейс сопоставление

#### **Алгоритм JOIN через штрихкоды**
```python
def cross_marketplace_join(wb_df: pd.DataFrame, oz_df: pd.DataFrame) -> pd.DataFrame:
    """
    Выполняет JOIN между нормализованными данными WB и Ozon
    через общие штрихкоды
    """
    # Стандартизация названий колонок
    wb_df_clean = wb_df.rename(columns={'individual_barcode_wb': 'barcode'})
    oz_df_clean = oz_df.rename(columns={'oz_barcode': 'barcode'})
    
    # Нормализация типов данных
    wb_df_clean['barcode'] = wb_df_clean['barcode'].astype(str).str.strip()
    oz_df_clean['barcode'] = oz_df_clean['barcode'].astype(str).str.strip()
    wb_df_clean['wb_sku'] = wb_df_clean['wb_sku'].astype(str)
    oz_df_clean['oz_sku'] = oz_df_clean['oz_sku'].astype(str)
    
    # Фильтрация пустых штрихкодов
    wb_df_clean = wb_df_clean[wb_df_clean['barcode'] != ''].drop_duplicates()
    oz_df_clean = oz_df_clean[oz_df_clean['barcode'] != ''].drop_duplicates()
    
    # JOIN по общим штрихкодам
    matched_df = pd.merge(
        wb_df_clean, 
        oz_df_clean, 
        on='barcode', 
        how='inner'
    )
    
    return matched_df
```

#### **Агрегация результатов**
```python
def aggregate_matches(matched_df: pd.DataFrame, selected_fields: dict) -> pd.DataFrame:
    """
    Агрегирует найденные соответствия и формирует итоговый результат
    """
    if matched_df.empty:
        return pd.DataFrame()
    
    # Группировка по WB SKU для подсчета соответствий
    match_stats = matched_df.groupby('wb_sku').agg({
        'oz_sku': 'nunique',  # Количество уникальных Ozon SKU
        'barcode': 'first'    # Первый найденный общий штрихкод
    }).rename(columns={
        'oz_sku': 'matched_oz_count',
        'barcode': 'common_matched_barcode'
    })
    
    # Объединение с исходными данными
    result_df = matched_df.merge(
        match_stats, 
        left_on='wb_sku', 
        right_index=True, 
        how='left'
    )
    
    # Фильтрация колонок согласно selected_fields
    final_columns = []
    for display_name, internal_name in selected_fields.items():
        if internal_name in result_df.columns:
            final_columns.append(internal_name)
            result_df[display_name] = result_df[internal_name]
    
    return result_df[list(selected_fields.keys())].drop_duplicates()
```

## 🔎 Алгоритмы поиска по критериям

### Поиск по WB SKU

#### **Прямой поиск**
```python
def search_by_wb_sku(con, wb_skus: List[str], selected_fields: dict) -> pd.DataFrame:
    """
    Поиск соответствий начиная от WB SKU
    
    Алгоритм:
    1. Получение нормализованных штрихкодов WB
    2. Поиск всех штрихкодов Ozon  
    3. JOIN по общим штрихкодам
    4. Агрегация результатов
    """
    # Шаг 1: Нормализация WB штрихкодов
    wb_normalized = get_normalized_wb_barcodes(con, wb_skus)
    if wb_normalized.empty:
        return pd.DataFrame()
    
    # Шаг 2: Получение всех Ozon штрихкодов
    oz_all = get_ozon_barcodes_and_identifiers(con)
    if oz_all.empty:
        return pd.DataFrame()
    
    # Шаг 3: Сопоставление
    matches = cross_marketplace_join(wb_normalized, oz_all)
    
    # Шаг 4: Агрегация
    return aggregate_matches(matches, selected_fields)
```

### Поиск по Ozon SKU

#### **Обратный поиск**
```python
def search_by_oz_sku(con, oz_skus: List[str], selected_fields: dict) -> pd.DataFrame:
    """
    Поиск соответствий начиная от Ozon SKU
    
    Алгоритм:
    1. Получение штрихкодов для указанных Ozon SKU
    2. Получение всех нормализованных WB штрихкодов
    3. JOIN по общим штрихкодам
    4. Агрегация результатов
    """
    # Шаг 1: Ozon штрихкоды для указанных SKU
    oz_filtered = get_ozon_barcodes_and_identifiers(con, oz_skus=oz_skus)
    if oz_filtered.empty:
        return pd.DataFrame()
    
    # Шаг 2: Все WB штрихкоды
    wb_all = get_normalized_wb_barcodes(con)
    if wb_all.empty:
        return pd.DataFrame()
    
    # Шаг 3: Сопоставление
    matches = cross_marketplace_join(wb_all, oz_filtered)
    
    # Шаг 4: Агрегация
    return aggregate_matches(matches, selected_fields)
```

### Поиск по артикулу Ozon

#### **Поиск через vendor_code**
```python
def search_by_vendor_code(con, vendor_codes: List[str], selected_fields: dict) -> pd.DataFrame:
    """
    Поиск через артикулы Ozon (vendor_code)
    
    Особенность: vendor_code может не совпадать с WB SKU,
    поэтому поиск идет только через штрихкоды
    """
    # Получение Ozon продуктов по артикулам
    oz_by_vendor = get_ozon_barcodes_and_identifiers(con, oz_vendor_codes=vendor_codes)
    if oz_by_vendor.empty:
        return pd.DataFrame()
    
    # Получение всех WB штрихкодов
    wb_all = get_normalized_wb_barcodes(con)
    if wb_all.empty:
        return pd.DataFrame()
    
    # Сопоставление через штрихкоды
    matches = cross_marketplace_join(wb_all, oz_by_vendor)
    
    return aggregate_matches(matches, selected_fields)
```

### Прямой поиск по штрихкоду

#### **Точное сопоставление**
```python
def search_by_barcode(con, barcodes: List[str], selected_fields: dict) -> pd.DataFrame:
    """
    Прямой поиск по штрихкоду в обеих системах
    
    Самый точный метод поиска
    """
    results = []
    
    for barcode in barcodes:
        clean_barcode = str(barcode).strip()
        if not validate_barcode(clean_barcode):
            continue
        
        # Поиск в WB
        wb_query = """
        WITH split_barcodes AS (
            SELECT 
                p.wb_sku,
                UNNEST(string_split(p.wb_barcodes, ';')) AS individual_barcode
            FROM wb_products p
        )
        SELECT wb_sku, TRIM(individual_barcode) as barcode
        FROM split_barcodes
        WHERE TRIM(individual_barcode) = ?
        """
        
        wb_matches = con.execute(wb_query, [clean_barcode]).fetchdf()
        
        # Поиск в Ozon
        oz_query = """
        SELECT DISTINCT
            b.oz_barcode,
            p.oz_sku, 
            p.oz_vendor_code, 
            p.oz_product_id
        FROM oz_barcodes b
        LEFT JOIN oz_products p ON b.oz_product_id = p.oz_product_id
        WHERE TRIM(b.oz_barcode) = ?
        """
        
        oz_matches = con.execute(oz_query, [clean_barcode]).fetchdf()
        
        # Формирование результата для данного штрихкода
        if not wb_matches.empty and not oz_matches.empty:
            for _, wb_row in wb_matches.iterrows():
                for _, oz_row in oz_matches.iterrows():
                    results.append({
                        'wb_sku': wb_row['wb_sku'],
                        'oz_sku': oz_row['oz_sku'],
                        'oz_vendor_code': oz_row['oz_vendor_code'],
                        'oz_product_id': oz_row['oz_product_id'],
                        'common_matched_barcode': clean_barcode
                    })
    
    if results:
        result_df = pd.DataFrame(results)
        return aggregate_matches(result_df, selected_fields)
    
    return pd.DataFrame()
```

## 🔧 Оптимизация поиска

### Кэширование результатов

#### **Кэш нормализованных штрихкодов**
```python
@lru_cache(maxsize=100)
def cached_get_normalized_wb_barcodes(connection_id: str, wb_skus_tuple: tuple) -> pd.DataFrame:
    """
    Кэширует результаты нормализации WB штрихкодов
    
    Эффективно для повторных поисков по одним и тем же SKU
    """
    # connection_id используется для инвалидации кэша при смене соединения
    wb_skus_list = list(wb_skus_tuple) if wb_skus_tuple else None
    return get_normalized_wb_barcodes(get_db_connection(), wb_skus_list)
```

#### **Индексирование штрихкодов**
```sql
-- Рекомендуемые индексы для оптимизации поиска
CREATE INDEX IF NOT EXISTS idx_oz_barcodes_barcode ON oz_barcodes(oz_barcode);
CREATE INDEX IF NOT EXISTS idx_oz_products_sku ON oz_products(oz_sku);
CREATE INDEX IF NOT EXISTS idx_oz_products_vendor_code ON oz_products(oz_vendor_code);
CREATE INDEX IF NOT EXISTS idx_wb_products_sku ON wb_products(wb_sku);

-- Для полнотекстового поиска по штрихкодам WB
CREATE INDEX IF NOT EXISTS idx_wb_products_barcodes_gin 
ON wb_products USING gin(to_tsvector('simple', wb_barcodes));
```

### Батчевая обработка

#### **Пакетная обработка больших списков**
```python
def batch_search(con, search_values: List[str], batch_size: int = 1000, 
                search_function: callable = search_by_wb_sku) -> pd.DataFrame:
    """
    Обрабатывает большие списки значений батчами
    
    Предотвращает превышение лимитов SQL запросов
    """
    all_results = []
    
    for i in range(0, len(search_values), batch_size):
        batch = search_values[i:i + batch_size]
        
        try:
            batch_result = search_function(con, batch)
            if not batch_result.empty:
                all_results.append(batch_result)
                
        except Exception as e:
            st.warning(f"Ошибка обработки батча {i//batch_size + 1}: {e}")
            continue
    
    if all_results:
        return pd.concat(all_results, ignore_index=True).drop_duplicates()
    
    return pd.DataFrame()
```

### Мониторинг производительности

#### **Сбор метрик поиска**
```python
def collect_search_metrics(search_type: str, search_values: List[str], 
                         result_df: pd.DataFrame, execution_time: float) -> Dict:
    """
    Собирает метрики производительности поиска
    """
    total_input = len(search_values)
    total_matches = len(result_df) if not result_df.empty else 0
    unique_wb_skus = result_df['wb_sku'].nunique() if 'wb_sku' in result_df.columns else 0
    unique_oz_skus = result_df['oz_sku'].nunique() if 'oz_sku' in result_df.columns else 0
    
    return {
        'search_type': search_type,
        'input_count': total_input,
        'match_count': total_matches,
        'match_rate': (total_matches / total_input * 100) if total_input > 0 else 0,
        'unique_wb_skus_found': unique_wb_skus,
        'unique_oz_skus_found': unique_oz_skus,
        'execution_time_seconds': execution_time,
        'performance_score': total_matches / execution_time if execution_time > 0 else 0
    }
```

## 🚨 Обработка ошибок

### Типы ошибок поиска

#### **1. Ошибки данных**
```python
class InvalidBarcodeError(Exception):
    """Некорректный формат штрихкода"""
    pass

class EmptySearchResultError(Exception):
    """Пустой результат поиска"""
    pass
```

#### **2. Ошибки соединения**
```python
class DatabaseConnectionError(Exception):
    """Ошибка соединения с базой данных"""
    pass

class QueryExecutionError(Exception):
    """Ошибка выполнения SQL запроса"""
    pass
```

#### **3. Обработка исключений**
```python
def safe_search_execution(search_function: callable, *args, **kwargs) -> pd.DataFrame:
    """
    Безопасное выполнение поиска с обработкой ошибок
    """
    try:
        result = search_function(*args, **kwargs)
        
        if result.empty:
            st.info("Соответствия не найдены по заданным критериям")
        
        return result
        
    except DatabaseConnectionError as e:
        st.error(f"Ошибка соединения с базой данных: {e}")
        return pd.DataFrame()
        
    except QueryExecutionError as e:
        st.error(f"Ошибка выполнения запроса: {e}")
        return pd.DataFrame()
        
    except Exception as e:
        st.error(f"Неожиданная ошибка поиска: {e}")
        return pd.DataFrame()
```

---

## 📝 Метаданные

**Модуль**: `utils/db_search_helpers.py`  
**Размер**: 361 строка кода  
**Последнее обновление**: 2024-12-19  
**Версия**: 1.2.0  
**Статус**: Стабильный  

**Связанные документы**:
- [Архитектура системы](../architecture-overview.md)
- [Database Utils API](../api/database-utils.md)
- [Связи в базе данных](../../data-structures/database-relations.md)

*Поисковые алгоритмы обеспечивают высокоточное сопоставление товаров между маркетплейсами через множественные критерии.* 