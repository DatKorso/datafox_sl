# 🗄️ Database Utils API

> Документация утилит для работы с базой данных DuckDB

## 📋 Обзор

Модули `utils/db_*` предоставляют полный набор утилит для работы с базой данных DuckDB, включая подключение, схему, CRUD операции и обслуживание.

## 🔌 db_connection.py

### Основные функции подключения

#### `connect_db(db_path: str = None) -> duckdb.DuckDBPyConnection | None`

Устанавливает соединение с базой данных DuckDB.

**Параметры:**
- `db_path` (str, optional): Путь к файлу БД. Если не указан, берется из конфигурации

**Возвращает:**
- `DuckDBPyConnection` при успехе
- `None` при ошибке

**Пример использования:**
```python
from utils.db_connection import connect_db

# Подключение с путем по умолчанию
conn = connect_db()
if conn:
    result = conn.execute("SELECT COUNT(*) FROM oz_products").fetchall()
    conn.close()

# Подключение к конкретному файлу
conn = connect_db("data/custom.duckdb")
```

**Особенности:**
- Автоматически создает директорию для БД, если не существует
- Интегрирован с Streamlit для отображения ошибок
- Поддерживает как Streamlit, так и standalone контексты

#### `test_db_connection(db_path: str = None) -> bool`

Проверяет соединение с базой данных.

**Параметры:**
- `db_path` (str, optional): Путь к файлу БД

**Возвращает:**
- `bool`: True если соединение успешно

**Пример:**
```python
if test_db_connection():
    st.success("База данных доступна")
else:
    st.error("Проблемы с подключением к БД")
```

#### `get_connection_and_ensure_schema()` [@st.cache_resource]

Кешированная функция для получения соединения и обеспечения схемы.

**Возвращает:**
- Кешированное соединение DuckDB
- Автоматически создает таблицы при первом вызове

**Пример:**
```python
@st.cache_resource
def get_cached_connection():
    return get_connection_and_ensure_schema()

conn = get_cached_connection()
```

## 🏗️ db_schema.py

### Жестко заданная схема

#### `HARDCODED_SCHEMA: dict`

Основная структура данных, определяющая схему всех таблиц.

**Структура:**
```python
{
    "table_name": {
        "description": "Описание таблицы",
        "file_type": "csv|xlsx|folder_xlsx|google_sheets",
        "read_params": {...},  # Параметры для pandas
        "config_report_key": "key_in_config",
        "columns": [
            {
                'target_col_name': 'column_name',
                'sql_type': 'VARCHAR|INTEGER|DOUBLE|BIGINT|DATE',
                'source_col_name': 'Source Column Name',
                'notes': 'Дополнительные заметки'
            }
        ],
        "pre_update_action": "SQL команда перед импортом"
    }
}
```

**Поддерживаемые типы файлов:**
- `csv` - CSV файлы с разделителями
- `xlsx` - Excel файлы с листами
- `folder_xlsx` - Папка с Excel файлами  
- `google_sheets` - Google Sheets API

### Основные функции схемы

#### `get_defined_table_names() -> list[str]`

Возвращает список всех определенных таблиц.

**Пример:**
```python
tables = get_defined_table_names()
# ['oz_orders', 'oz_products', 'wb_products', ...]
```

#### `get_table_schema_definition(table_name: str) -> dict | None`

Получает полное определение схемы для таблицы.

**Параметры:**
- `table_name` (str): Название таблицы

**Возвращает:**
- `dict`: Полное определение схемы
- `None`: Если таблица не найдена

**Пример:**
```python
schema = get_table_schema_definition("oz_products")
if schema:
    columns = schema["columns"]
    file_type = schema["file_type"]
```

#### `get_table_columns_from_schema(table_name: str) -> list[tuple]`

Извлекает информацию о колонках таблицы.

**Возвращает:**
```python
[
    (target_col_name, sql_type, source_col_name, notes),
    ...
]
```

**Пример:**
```python
columns = get_table_columns_from_schema("oz_orders")
for col_name, sql_type, source_name, notes in columns:
    print(f"{col_name} ({sql_type}) <- {source_name}")
```

#### `is_dynamic_schema_table(table_name: str) -> bool`

Проверяет, является ли таблица динамической (например, Google Sheets).

#### `create_tables_from_schema(con: duckdb.DuckDBPyConnection) -> bool`

Создает все таблицы согласно схеме.

**Пример:**
```python
conn = connect_db()
if create_tables_from_schema(conn):
    st.success("Все таблицы созданы")
```

## 🔧 db_crud.py

### CRUD операции

#### Создание и обновление данных

##### `insert_dataframe_to_table(con, df, table_name, batch_size=1000) -> bool`

Вставляет DataFrame в таблицу пакетами.

**Параметры:**
- `con`: Соединение DuckDB
- `df`: pandas DataFrame
- `table_name`: Название таблицы
- `batch_size`: Размер пакета для вставки

**Пример:**
```python
conn = connect_db()
df = pd.read_csv("data.csv")
success = insert_dataframe_to_table(conn, df, "oz_products")
```

##### `update_table_from_schema(con, df, table_name) -> bool`

Обновляет таблицу согласно схеме с предварительной очисткой.

**Процесс:**
1. Выполняет `pre_update_action` (обычно DELETE)
2. Вставляет новые данные
3. Валидирует результат

#### Чтение данных

##### `get_table_row_count(con, table_name) -> int`

Получает количество записей в таблице.

**Пример:**
```python
count = get_table_row_count(conn, "oz_products")
st.metric("Товары Ozon", count)
```

##### `get_paginated_table_data(con, table_name, offset=0, limit=100) -> pd.DataFrame`

Получает данные таблицы с пагинацией.

**Параметры:**
- `offset`: Смещение (для пагинации)
- `limit`: Максимальное количество записей

**Пример:**
```python
# Получить первые 50 записей
df = get_paginated_table_data(conn, "oz_products", offset=0, limit=50)

# Следующие 50 записей  
df_next = get_paginated_table_data(conn, "oz_products", offset=50, limit=50)
```

##### `search_in_table(con, table_name, search_term, columns=None) -> pd.DataFrame`

Выполняет поиск по таблице.

**Параметры:**
- `search_term`: Поисковый запрос
- `columns`: Список колонок для поиска (если None - по всем)

**Пример:**
```python
# Поиск по всем колонкам
results = search_in_table(conn, "oz_products", "iPhone")

# Поиск в конкретных колонках
results = search_in_table(conn, "oz_products", "Apple", 
                         columns=["oz_brand", "product_name"])
```

#### Аналитические функции

##### `get_table_statistics(con, table_name) -> dict`

Получает статистику по таблице.

**Возвращает:**
```python
{
    "row_count": 1000,
    "column_count": 15,
    "size_mb": 2.5,
    "last_updated": "2024-12-19"
}
```

##### `get_column_value_counts(con, table_name, column_name, limit=10) -> pd.DataFrame`

Получает топ значений для колонки.

**Пример:**
```python
# Топ-10 брендов
top_brands = get_column_value_counts(conn, "oz_products", "oz_brand")
```

## 🧹 db_cleanup.py

### Обслуживание базы данных

#### `vacuum_database(con) -> bool`

Выполняет сжатие базы данных.

**Пример:**
```python
if vacuum_database(conn):
    st.success("База данных оптимизирована")
```

#### `analyze_database(con) -> bool`

Обновляет статистику таблиц для оптимизатора.

#### `check_database_integrity(con) -> dict`

Проверяет целостность базы данных.

**Возвращает:**
```python
{
    "status": "OK|ERROR",
    "issues": [],
    "recommendations": []
}
```

#### `get_database_size_info(con) -> dict`

Получает информацию о размере БД.

**Возвращает:**
```python
{
    "total_size_mb": 125.6,
    "table_sizes": {
        "oz_products": 45.2,
        "wb_products": 32.1,
        ...
    }
}
```

## 🔄 db_migration.py

### Миграции схемы

#### `get_current_schema_version(con) -> str`

Получает текущую версию схемы.

#### `apply_migration(con, migration_script) -> bool`

Применяет скрипт миграции.

**Пример:**
```python
migration_sql = """
ALTER TABLE oz_products ADD COLUMN new_field VARCHAR;
UPDATE schema_version SET version = '1.1.0';
"""
apply_migration(conn, migration_sql)
```

#### `backup_database(con, backup_path) -> bool`

Создает резервную копию БД.

## 📊 Интеграция с Streamlit

### Кеширование запросов

```python
@st.cache_data
def cached_table_data(table_name, _conn):
    """Кешированное получение данных таблицы"""
    return get_paginated_table_data(_conn, table_name)

@st.cache_data
def cached_search(_conn, table_name, search_term):
    """Кешированный поиск"""
    return search_in_table(_conn, table_name, search_term)
```

### Отображение прогресса

```python
def import_with_progress(df, table_name):
    """Импорт с отображением прогресса"""
    progress_bar = st.progress(0)
    
    total_rows = len(df)
    batch_size = 1000
    
    for i in range(0, total_rows, batch_size):
        batch_df = df[i:i+batch_size]
        insert_dataframe_to_table(conn, batch_df, table_name)
        
        progress = min((i + batch_size) / total_rows, 1.0)
        progress_bar.progress(progress)
    
    st.success(f"Импортировано {total_rows} записей")
```

## 🚨 Обработка ошибок

### Типовые ошибки

#### `DatabaseConnectionError`
```python
try:
    conn = connect_db()
except DatabaseConnectionError:
    st.error("Не удалось подключиться к базе данных")
    st.stop()
```

#### `SchemaValidationError`
```python
try:
    update_table_from_schema(conn, df, "oz_products")
except SchemaValidationError as e:
    st.error(f"Ошибка схемы: {e}")
```

#### `DataIntegrityError`
```python
try:
    insert_dataframe_to_table(conn, df, table_name)
except DataIntegrityError as e:
    st.warning(f"Проблемы с данными: {e}")
```

### Валидация данных

```python
def validate_before_insert(df, table_name):
    """Валидация DataFrame перед вставкой"""
    schema = get_table_schema_definition(table_name)
    required_columns = [col['target_col_name'] for col in schema['columns']]
    
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Отсутствуют колонки: {missing_columns}")
    
    # Проверка типов данных
    for col_info in schema['columns']:
        col_name = col_info['target_col_name']
        sql_type = col_info['sql_type']
        
        if col_name in df.columns:
            validate_column_type(df[col_name], sql_type)
```

## 📈 Мониторинг производительности

### Логирование запросов

```python
import time
import logging

def execute_with_timing(conn, query, description=""):
    """Выполнение запроса с измерением времени"""
    start_time = time.time()
    
    try:
        result = conn.execute(query).fetchall()
        execution_time = time.time() - start_time
        
        logging.info(f"{description}: {execution_time:.2f}s")
        
        if execution_time > 5.0:  # Медленный запрос
            st.warning(f"Медленный запрос: {execution_time:.2f}s")
            
        return result
        
    except Exception as e:
        logging.error(f"Ошибка запроса {description}: {e}")
        raise
```

### Метрики производительности

```python
def display_db_metrics(conn):
    """Отображение метрик БД в Streamlit"""
    col1, col2, col3 = st.columns(3)
    
    with col1:
        size_info = get_database_size_info(conn)
        st.metric("Размер БД", f"{size_info['total_size_mb']:.1f} MB")
    
    with col2:
        total_records = sum(get_table_row_count(conn, table) 
                          for table in get_defined_table_names())
        st.metric("Всего записей", f"{total_records:,}")
    
    with col3:
        integrity = check_database_integrity(conn)
        status_color = "green" if integrity["status"] == "OK" else "red"
        st.metric("Статус БД", integrity["status"])
```

---

## 📝 Метаданные

**Модули**: `db_connection.py`, `db_schema.py`, `db_crud.py`, `db_cleanup.py`, `db_migration.py`  
**Последнее обновление**: 2024-12-19  
**Версия API**: 1.2.0  
**Статус**: Стабильный  

**Связанные документы**:
- [Архитектура системы](../architecture-overview.md)
- [Схема базы данных](../../data-structures/schemas/db_schema.md)
- [Data Helpers API](data-helpers.md) 