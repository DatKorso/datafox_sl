# 📥 Система импорта данных

> Техническая документация архитектуры и реализации импорта данных маркетплейсов

## 🎯 Обзор системы

Система импорта DataFox SL обеспечивает загрузку и обработку данных из различных источников маркетплейсов в единую базу данных DuckDB с валидацией, трансформацией и контролем качества.

## 🏗️ Архитектура импорта

### Типы источников данных

#### **1. CSV файлы (Ozon)**
```python
# Заказы Ozon
file_type: "csv"
read_params: {
    'delimiter': ';',
    'header': 0,
    'encoding': 'utf-8'
}
```

#### **2. XLSX файлы (одиночные)**
```python
# Цены WB, штрихкоды Ozon
file_type: "xlsx"
read_params: {
    'sheet_name': "Штрихкоды",
    'header': 2,
    'skip_rows_after_header': 1
}
```

#### **3. Папки XLSX файлов**
```python
# Категории Ozon, товары WB
file_type: "folder_xlsx"
read_params: {
    'sheet_name': "Шаблон",
    'header': 1,
    'skip_rows_after_header': 2
}
```

#### **4. Google Sheets API**
```python
# Справочник Punta
file_type: "google_sheets"
read_params: {
    'credentials_path': 'credentials.json',
    'range': 'A1:Z1000'
}
```

### Поток обработки данных

Процесс импорта включает несколько этапов:

1. **Загрузка** - Чтение данных из источника
2. **Валидация** - Проверка схемы и типов данных  
3. **Трансформация** - Преобразование в целевой формат
4. **Очистка** - Удаление существующих данных
5. **Вставка** - Загрузка новых данных в БД
6. **Валидация** - Проверка результата

## 📝 Жестко заданная схема

### Структура HARDCODED_SCHEMA

```python
HARDCODED_SCHEMA = {
    "table_name": {
        "description": "Описание таблицы",
        "file_type": "csv|xlsx|folder_xlsx|google_sheets",
        "read_params": {
            # Параметры для pandas.read_*
        },
        "config_report_key": "key_in_config_json",
        "columns": [
            {
                'target_col_name': 'db_column_name',
                'sql_type': 'VARCHAR|INTEGER|DOUBLE|BIGINT|DATE',
                'source_col_name': 'Original Column Name',
                'notes': 'transformation_rules'
            }
        ],
        "pre_update_action": "DELETE FROM table_name;"
    }
}
```

### Примеры определений

#### **Ozon Products (CSV)**
```python
"oz_products": {
    "description": "Товары Ozon",
    "file_type": "csv",
    "read_params": {
        'delimiter': ';', 
        'header': 0
    },
    "config_report_key": "oz_products_csv",
    "columns": [
        {
            'target_col_name': 'oz_vendor_code',
            'sql_type': 'VARCHAR',
            'source_col_name': 'Артикул',
            'notes': "remove_single_quotes"
        },
        {
            'target_col_name': 'oz_sku',
            'sql_type': 'BIGINT',
            'source_col_name': 'SKU',
            'notes': None
        }
    ],
    "pre_update_action": "DELETE FROM oz_products;"
}
```

#### **WB Products (Folder XLSX)**
```python
"wb_products": {
    "description": "Товары Wildberries",
    "file_type": "folder_xlsx",
    "read_params": {
        'sheet_name': "Товары",
        'header': 2,
        'skip_rows_after_header': 1
    },
    "config_report_key": "wb_products_dir",
    "columns": [
        {
            'target_col_name': 'wb_sku',
            'sql_type': 'INTEGER',
            'source_col_name': 'Артикул WB',
            'notes': None
        }
    ],
    "pre_update_action": "DELETE FROM wb_products;"
}
```

## 🔧 Реализация импорта

### Основные компоненты

#### **1. Валидация схемы**
```python
def validate_import_schema(df: pd.DataFrame, table_name: str) -> list[str]:
    """
    Валидирует DataFrame против жестко заданной схемы
    
    Returns:
        list[str]: Список ошибок валидации
    """
    schema = get_table_schema_definition(table_name)
    if not schema:
        return [f"Схема для таблицы {table_name} не найдена"]
    
    errors = []
    required_columns = [col['source_col_name'] for col in schema['columns']]
    
    # Проверка наличия обязательных колонок
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        errors.append(f"Отсутствуют колонки: {missing_columns}")
    
    # Проверка типов данных
    for col_info in schema['columns']:
        source_col = col_info['source_col_name']
        sql_type = col_info['sql_type']
        
        if source_col in df.columns:
            validation_error = validate_column_type(df[source_col], sql_type)
            if validation_error:
                errors.append(f"{source_col}: {validation_error}")
    
    return errors
```

#### **2. Трансформация данных**
```python
def transform_dataframe_for_import(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Трансформирует DataFrame согласно правилам схемы
    """
    schema = get_table_schema_definition(table_name)
    transformed_df = df.copy()
    
    for col_info in schema['columns']:
        source_col = col_info['source_col_name']
        target_col = col_info['target_col_name']
        notes = col_info.get('notes')
        
        if source_col in transformed_df.columns:
            # Переименование колонки
            if source_col != target_col:
                transformed_df.rename(columns={source_col: target_col}, inplace=True)
            
            # Применение трансформаций
            if notes:
                transformed_df[target_col] = apply_transformation(
                    transformed_df[target_col], notes
                )
    
    # Удаление лишних колонок
    target_columns = [col['target_col_name'] for col in schema['columns']]
    extra_columns = set(transformed_df.columns) - set(target_columns)
    transformed_df.drop(columns=extra_columns, inplace=True)
    
    return transformed_df
```

#### **3. Трансформации по notes**
```python
def apply_transformation(series: pd.Series, transformation: str) -> pd.Series:
    """
    Применяет трансформацию к серии данных
    """
    if transformation == "remove_single_quotes":
        return series.astype(str).str.replace("'", "")
    
    elif transformation == "convert_to_date":
        return pd.to_datetime(series, errors='coerce')
    
    elif transformation == "round_to_integer":
        return series.round().astype('Int64')
    
    elif transformation == "normalize_wb_barcodes":
        # Для WB: "123;456;789" -> нормализация в отдельные записи
        return series.astype(str).str.strip()
    
    else:
        return series
```

### Специализированные загрузчики

#### **CSV Loader**
```python
def load_csv_file(file_path: str, read_params: dict) -> pd.DataFrame:
    """
    Загрузка CSV файлов с обработкой кодировок
    """
    try:
        # Попытка с UTF-8
        df = pd.read_csv(file_path, **read_params, encoding='utf-8')
    except UnicodeDecodeError:
        try:
            # Fallback на Windows-1251
            df = pd.read_csv(file_path, **read_params, encoding='windows-1251')
        except UnicodeDecodeError:
            # Последняя попытка с автоопределением
            df = pd.read_csv(file_path, **read_params, encoding='utf-8-sig')
    
    return df
```

#### **XLSX Loader**
```python
def load_xlsx_file(file_path: str, read_params: dict) -> pd.DataFrame:
    """
    Загрузка Excel файлов с обработкой специфичных параметров
    """
    sheet_name = read_params.get('sheet_name', 0)
    header = read_params.get('header', 0)
    skip_rows = read_params.get('skip_rows_after_header', 0)
    
    # Базовая загрузка
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=header)
    
    # Пропуск строк после заголовка
    if skip_rows > 0:
        df = df.iloc[skip_rows:].reset_index(drop=True)
    
    # Очистка пустых строк
    df = df.dropna(how='all')
    
    return df
```

#### **Folder XLSX Loader**
```python
def load_folder_xlsx(folder_path: str, read_params: dict) -> pd.DataFrame:
    """
    Загрузка всех XLSX файлов из папки с объединением
    """
    xlsx_files = glob.glob(os.path.join(folder_path, "*.xlsx"))
    
    if not xlsx_files:
        raise ValueError(f"Не найдено XLSX файлов в папке: {folder_path}")
    
    dataframes = []
    
    for file_path in xlsx_files:
        try:
            df = load_xlsx_file(file_path, read_params)
            
            # Добавляем метаданные о файле
            df['_source_file'] = os.path.basename(file_path)
            df['_import_date'] = datetime.now()
            
            dataframes.append(df)
            
        except Exception as e:
            st.warning(f"Ошибка загрузки {file_path}: {e}")
            continue
    
    if not dataframes:
        raise ValueError("Не удалось загрузить ни одного файла")
    
    # Объединение всех DataFrame
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    return combined_df
```

#### **Google Sheets Loader**
```python
def load_google_sheets(sheets_url: str, credentials_path: str) -> pd.DataFrame:
    """
    Загрузка данных из Google Sheets
    """
    try:
        # Инициализация Google Sheets API
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            credentials_path, scope
        )
        gc = gspread.authorize(credentials)
        
        # Извлечение ID таблицы из URL
        sheet_id = extract_sheet_id_from_url(sheets_url)
        sheet = gc.open_by_key(sheet_id)
        
        # Загрузка первого листа
        worksheet = sheet.get_worksheet(0)
        data = worksheet.get_all_records()
        
        df = pd.DataFrame(data)
        
        # Очистка пустых строк и колонок
        df = df.dropna(how='all').dropna(axis=1, how='all')
        
        return df
        
    except Exception as e:
        raise ConnectionError(f"Ошибка загрузки Google Sheets: {e}")
```

## 🔄 Процесс импорта

### Главная функция импорта

```python
def import_data_to_table(source_data, table_name: str, source_type: str) -> dict:
    """
    Основная функция импорта данных в таблицу
    
    Args:
        source_data: Файл, путь к папке или URL
        table_name: Название таблицы в схеме
        source_type: Тип источника данных
        
    Returns:
        dict: Результат импорта с метриками
    """
    result = {
        'success': False,
        'errors': [],
        'warnings': [],
        'imported_rows': 0,
        'processing_time': 0
    }
    
    start_time = time.time()
    
    try:
        # 1. Получение схемы
        schema = get_table_schema_definition(table_name)
        if not schema:
            result['errors'].append(f"Схема для {table_name} не найдена")
            return result
        
        # 2. Загрузка данных
        if schema['file_type'] == 'csv':
            df = load_csv_file(source_data, schema['read_params'])
        elif schema['file_type'] == 'xlsx':
            df = load_xlsx_file(source_data, schema['read_params'])
        elif schema['file_type'] == 'folder_xlsx':
            df = load_folder_xlsx(source_data, schema['read_params'])
        elif schema['file_type'] == 'google_sheets':
            df = load_google_sheets(source_data, schema['read_params'])
        else:
            result['errors'].append(f"Неподдерживаемый тип файла: {schema['file_type']}")
            return result
        
        # 3. Валидация схемы
        validation_errors = validate_import_schema(df, table_name)
        if validation_errors:
            result['errors'].extend(validation_errors)
            return result
        
        # 4. Трансформация данных
        transformed_df = transform_dataframe_for_import(df, table_name)
        
        # 5. Подключение к БД
        conn = connect_db()
        if not conn:
            result['errors'].append("Не удалось подключиться к базе данных")
            return result
        
        # 6. Pre-update действие
        if schema.get('pre_update_action'):
            conn.execute(schema['pre_update_action'])
        
        # 7. Вставка данных
        success = insert_dataframe_to_table(conn, transformed_df, table_name)
        
        if success:
            result['success'] = True
            result['imported_rows'] = len(transformed_df)
        else:
            result['errors'].append("Ошибка вставки данных в таблицу")
        
        conn.close()
        
    except Exception as e:
        result['errors'].append(f"Критическая ошибка импорта: {str(e)}")
    
    finally:
        result['processing_time'] = time.time() - start_time
    
    return result
```

### Batch обработка

```python
def import_multiple_files_batch(file_mappings: list[dict]) -> dict:
    """
    Пакетный импорт нескольких файлов
    
    Args:
        file_mappings: [
            {
                'file_path': '/path/to/file',
                'table_name': 'oz_products',
                'priority': 1
            }
        ]
    """
    results = {
        'total_files': len(file_mappings),
        'successful_imports': 0,
        'failed_imports': 0,
        'details': []
    }
    
    # Сортировка по приоритету
    sorted_mappings = sorted(file_mappings, key=lambda x: x.get('priority', 999))
    
    for mapping in sorted_mappings:
        file_result = import_data_to_table(
            mapping['file_path'],
            mapping['table_name'],
            'auto'
        )
        
        results['details'].append({
            'file': mapping['file_path'],
            'table': mapping['table_name'],
            'result': file_result
        })
        
        if file_result['success']:
            results['successful_imports'] += 1
        else:
            results['failed_imports'] += 1
    
    return results
```

## 📊 Мониторинг и логирование

### Метрики импорта

```python
def collect_import_metrics(result: dict, table_name: str) -> dict:
    """
    Сбор метрик импорта для мониторинга
    """
    return {
        'timestamp': datetime.now().isoformat(),
        'table_name': table_name,
        'imported_rows': result.get('imported_rows', 0),
        'processing_time_seconds': result.get('processing_time', 0),
        'success': result.get('success', False),
        'error_count': len(result.get('errors', [])),
        'warning_count': len(result.get('warnings', [])),
        'throughput_rows_per_second': (
            result.get('imported_rows', 0) / max(result.get('processing_time', 1), 0.001)
        )
    }
```

### Логирование процесса

```python
import logging

def setup_import_logging():
    """Настройка логирования для системы импорта"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/import.log'),
            logging.StreamHandler()
        ]
    )

def log_import_process(table_name: str, result: dict):
    """Логирование результата импорта"""
    logger = logging.getLogger('DataFoxImport')
    
    if result['success']:
        logger.info(
            f"Успешный импорт в {table_name}: "
            f"{result['imported_rows']} строк за {result['processing_time']:.2f}с"
        )
    else:
        logger.error(
            f"Ошибка импорта в {table_name}: {'; '.join(result['errors'])}"
        )
        
    if result.get('warnings'):
        logger.warning(
            f"Предупреждения при импорте {table_name}: {'; '.join(result['warnings'])}"
        )
```

## 🚨 Обработка ошибок

### Типы ошибок импорта

#### **1. Ошибки валидации**
```python
class SchemaValidationError(Exception):
    """Ошибка валидации схемы данных"""
    pass

class DataTypeError(Exception):
    """Ошибка типов данных"""
    pass
```

#### **2. Ошибки источников данных**
```python
class DataSourceError(Exception):
    """Ошибка доступа к источнику данных"""
    pass

class FileFormatError(Exception):
    """Ошибка формата файла"""
    pass
```

#### **3. Ошибки базы данных**
```python
class DatabaseError(Exception):
    """Ошибка работы с базой данных"""
    pass

class TableInsertError(Exception):
    """Ошибка вставки в таблицу"""
    pass
```

### Стратегии восстановления

```python
def import_with_retry(source_data, table_name: str, max_retries: int = 3) -> dict:
    """
    Импорт с повторными попытками
    """
    for attempt in range(max_retries):
        try:
            result = import_data_to_table(source_data, table_name, 'auto')
            
            if result['success']:
                return result
            
            # При неуспехе ждем перед повтором
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
                
        except Exception as e:
            if attempt == max_retries - 1:
                return {
                    'success': False,
                    'errors': [f"Все попытки исчерпаны: {str(e)}"],
                    'imported_rows': 0
                }
            time.sleep(2 ** attempt)
    
    return {'success': False, 'errors': ['Неизвестная ошибка'], 'imported_rows': 0}
```

## 🔧 Конфигурация импорта

### Настройки в config.json

```json
{
    "reports": {
        "oz_orders_csv": "/path/to/ozon/orders",
        "oz_products_csv": "/path/to/ozon/products.csv",
        "oz_barcodes_xlsx": "/path/to/ozon/barcodes.xlsx",
        "wb_products_dir": "/path/to/wb/products/",
        "wb_prices_xlsx": "/path/to/wb/prices.xlsx",
        "punta_google_sheets_url": "https://docs.google.com/spreadsheets/d/..."
    },
    "import_settings": {
        "batch_size": 1000,
        "max_retries": 3,
        "timeout_seconds": 300,
        "enable_logging": true,
        "backup_before_import": true
    }
}
```

### Валидация конфигурации

```python
def validate_import_config() -> list[str]:
    """
    Проверка корректности настроек импорта
    """
    errors = []
    config = load_config()
    
    # Проверка путей к файлам
    for table_name in get_defined_table_names():
        schema = get_table_schema_definition(table_name)
        config_key = schema.get('config_report_key')
        
        if config_key:
            path = config.get('reports', {}).get(config_key)
            if not path:
                errors.append(f"Не настроен путь для {table_name}: {config_key}")
            elif schema['file_type'] in ['csv', 'xlsx'] and not os.path.exists(path):
                errors.append(f"Файл не найден: {path}")
            elif schema['file_type'] == 'folder_xlsx' and not os.path.isdir(path):
                errors.append(f"Папка не найдена: {path}")
    
    return errors
```

---

## 📝 Метаданные

**Модули**: `pages/2_🖇_Импорт_Отчетов_МП.py`, `utils/data_cleaner.py`, `utils/db_schema.py`  
**Последнее обновление**: 2024-12-19  
**Версия**: 1.2.0  
**Статус**: Стабильный  

**Связанные документы**:
- [Архитектура системы](../architecture-overview.md)
- [Database Utils API](../api/database-utils.md)
- [Схема базы данных](../../data-structures/schemas/db_schema.md)

*Система импорта обеспечивает надежную загрузку данных из всех поддерживаемых источников маркетплейсов.* 