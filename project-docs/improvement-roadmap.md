# План улучшений проекта Marketplace Data Analyzer

## Приоритетные улучшения (Следующие 2 недели)

### 🔥 Критические исправления

#### 1. Улучшение логирования
**Проблема:** Использование `print()` вместо proper logging  
**Решение:** Внедрить модуль logging

```python
# utils/logger.py
import logging
import sys
from pathlib import Path

def setup_logger(name: str, log_file: str = None, level: int = logging.INFO):
    """Настройка логгера для модуля"""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Форматтер
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (опционально)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger
```

#### 2. Добавление валидации данных
**Проблема:** Недостаточная проверка входных данных  
**Решение:** Создать модуль валидации

```python
# utils/validators.py
from typing import Optional, List
import pandas as pd
import os

class DataValidator:
    @staticmethod
    def validate_file_path(path: str) -> tuple[bool, str]:
        """Валидация пути к файлу"""
        if not path:
            return False, "Путь к файлу не указан"
        
        if not os.path.exists(path):
            return False, f"Файл не найден: {path}"
        
        if not os.path.isfile(path):
            return False, f"Указанный путь не является файлом: {path}"
        
        return True, ""
    
    @staticmethod
    def validate_dataframe(df: pd.DataFrame, required_columns: List[str]) -> tuple[bool, str]:
        """Валидация DataFrame"""
        if df.empty:
            return False, "DataFrame пуст"
        
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            return False, f"Отсутствуют обязательные колонки: {missing_columns}"
        
        return True, ""
```

#### 3. Оптимизация производительности
**Проблема:** Отсутствие кэширования  
**Решение:** Добавить кэширование для тяжелых операций

```python
# В utils/db_crud.py
import streamlit as st

@st.cache_data(ttl=300)  # Кэш на 5 минут
def get_db_stats_cached(db_path: str) -> dict:
    """Кэшированная версия получения статистики БД"""
    from .db_connection import connect_db
    
    con = connect_db(db_path)
    if not con:
        return {}
    
    stats = get_db_stats(con)
    con.close()
    return stats

@st.cache_data
def load_table_data_cached(db_path: str, table_name: str, limit: int = 1000) -> pd.DataFrame:
    """Кэшированная загрузка данных таблицы"""
    from .db_connection import connect_db
    
    con = connect_db(db_path)
    if not con:
        return pd.DataFrame()
    
    try:
        query = f'SELECT * FROM "{table_name}" LIMIT {limit}'
        df = con.execute(query).df()
        return df
    except Exception as e:
        st.error(f"Ошибка загрузки данных: {e}")
        return pd.DataFrame()
    finally:
        con.close()
```

### 🛠️ Улучшения UX

#### 4. Добавление прогресс-баров
```python
# В pages/2_Import_Reports.py
def import_with_progress(file_path: str, table_name: str):
    """Импорт с отображением прогресса"""
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    try:
        # Шаг 1: Чтение файла
        status_text.text("Чтение файла...")
        progress_bar.progress(25)
        df = pd.read_csv(file_path)
        
        # Шаг 2: Валидация
        status_text.text("Валидация данных...")
        progress_bar.progress(50)
        # ... валидация
        
        # Шаг 3: Импорт
        status_text.text("Импорт в базу данных...")
        progress_bar.progress(75)
        # ... импорт
        
        # Завершение
        progress_bar.progress(100)
        status_text.text("Импорт завершен!")
        
    except Exception as e:
        st.error(f"Ошибка импорта: {e}")
    finally:
        progress_bar.empty()
        status_text.empty()
```

#### 5. Улучшение обработки ошибок
```python
# utils/error_handlers.py
import streamlit as st
import traceback
from functools import wraps

def handle_errors(func):
    """Декоратор для обработки ошибок"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            st.error(f"Произошла ошибка в функции {func.__name__}: {str(e)}")
            
            # В режиме разработки показываем полный traceback
            if st.session_state.get('debug_mode', False):
                st.code(traceback.format_exc())
            
            return None
    return wrapper
```

## Среднесрочные улучшения (1-2 месяца)

### 🏗️ Архитектурные изменения

#### 6. Система миграций БД
```python
# utils/migrations.py
from typing import List
import duckdb

class Migration:
    def __init__(self, version: str, description: str, sql: str):
        self.version = version
        self.description = description
        self.sql = sql

class MigrationManager:
    def __init__(self, connection: duckdb.DuckDBPyConnection):
        self.connection = connection
        self.migrations: List[Migration] = []
        self._ensure_migration_table()
    
    def _ensure_migration_table(self):
        """Создание таблицы для отслеживания миграций"""
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version VARCHAR PRIMARY KEY,
                description VARCHAR,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    def add_migration(self, migration: Migration):
        """Добавление миграции"""
        self.migrations.append(migration)
    
    def apply_pending_migrations(self):
        """Применение всех неприменённых миграций"""
        applied_versions = set(
            row[0] for row in 
            self.connection.execute("SELECT version FROM schema_migrations").fetchall()
        )
        
        for migration in self.migrations:
            if migration.version not in applied_versions:
                self._apply_migration(migration)
    
    def _apply_migration(self, migration: Migration):
        """Применение одной миграции"""
        try:
            self.connection.execute(migration.sql)
            self.connection.execute(
                "INSERT INTO schema_migrations (version, description) VALUES (?, ?)",
                [migration.version, migration.description]
            )
            print(f"Применена миграция {migration.version}: {migration.description}")
        except Exception as e:
            print(f"Ошибка применения миграции {migration.version}: {e}")
            raise
```

#### 7. Конфигурируемая схема
```python
# schemas/schema_loader.py
import yaml
from typing import Dict, Any

class SchemaLoader:
    @staticmethod
    def load_schema(schema_file: str) -> Dict[str, Any]:
        """Загрузка схемы из YAML файла"""
        with open(schema_file, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    @staticmethod
    def validate_schema(schema: Dict[str, Any]) -> bool:
        """Валидация схемы"""
        required_keys = ['tables']
        return all(key in schema for key in required_keys)
```

```yaml
# schemas/marketplace_schema.yaml
version: "1.0"
tables:
  oz_orders:
    description: "Заказы Ozon"
    file_type: "csv"
    read_params:
      delimiter: ";"
      header: 0
    columns:
      - name: oz_order_number
        type: VARCHAR
        source: "Номер заказа"
        required: true
      - name: oz_accepted_date
        type: DATE
        source: "Принят в обработку"
        transform: "convert_to_date"
```

### 🧪 Тестирование

#### 8. Unit тесты
```python
# tests/test_config_utils.py
import pytest
import tempfile
import os
from utils.config_utils import load_config, save_config

class TestConfigUtils:
    def test_load_default_config(self):
        """Тест загрузки конфигурации по умолчанию"""
        with tempfile.TemporaryDirectory() as temp_dir:
            os.chdir(temp_dir)
            config = load_config()
            assert 'database_path' in config
            assert 'report_paths' in config
    
    def test_save_and_load_config(self):
        """Тест сохранения и загрузки конфигурации"""
        test_config = {
            'database_path': 'test.db',
            'report_paths': {'test': 'path'}
        }
        
        with tempfile.TemporaryDirectory() as temp_dir:
            os.chdir(temp_dir)
            save_config(test_config)
            loaded_config = load_config()
            assert loaded_config == test_config
```

```python
# tests/test_db_operations.py
import pytest
import pandas as pd
from utils.db_crud import import_data_from_dataframe
from utils.db_connection import connect_db

class TestDBOperations:
    @pytest.fixture
    def sample_dataframe(self):
        return pd.DataFrame({
            'Номер заказа': ['12345', '67890'],
            'Статус': ['Доставлен', 'В пути']
        })
    
    def test_import_data_success(self, sample_dataframe):
        """Тест успешного импорта данных"""
        # Создание временной БД в памяти
        con = connect_db(':memory:')
        
        # Тест импорта
        success, count, error = import_data_from_dataframe(
            con, sample_dataframe, 'oz_orders'
        )
        
        assert success
        assert count == 2
        assert error == ""
```

## Долгосрочные улучшения (3-6 месяцев)

### 🚀 Масштабирование

#### 9. Plugin система
```python
# utils/plugin_manager.py
from abc import ABC, abstractmethod
from typing import Dict, Any

class MarketplacePlugin(ABC):
    @abstractmethod
    def get_name(self) -> str:
        pass
    
    @abstractmethod
    def get_schema(self) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def process_data(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        pass

class PluginManager:
    def __init__(self):
        self.plugins: Dict[str, MarketplacePlugin] = {}
    
    def register_plugin(self, plugin: MarketplacePlugin):
        self.plugins[plugin.get_name()] = plugin
    
    def get_available_marketplaces(self) -> List[str]:
        return list(self.plugins.keys())
```

#### 10. Асинхронные операции
```python
# utils/async_operations.py
import asyncio
import aiofiles
import pandas as pd
from typing import List

class AsyncDataProcessor:
    @staticmethod
    async def process_multiple_files(file_paths: List[str]) -> List[pd.DataFrame]:
        """Асинхронная обработка нескольких файлов"""
        tasks = [
            AsyncDataProcessor._process_single_file(path) 
            for path in file_paths
        ]
        return await asyncio.gather(*tasks)
    
    @staticmethod
    async def _process_single_file(file_path: str) -> pd.DataFrame:
        """Асинхронная обработка одного файла"""
        # Имитация тяжелой операции
        await asyncio.sleep(0.1)
        return pd.read_csv(file_path)
```

## Метрики успеха

### Краткосрочные (2 недели)
- [ ] Все `print()` заменены на proper logging
- [ ] Добавлена валидация входных данных
- [ ] Внедрено кэширование для основных операций
- [ ] Добавлены прогресс-бары для длительных операций

### Среднесрочные (2 месяца)
- [ ] Создана система миграций БД
- [ ] Схема вынесена в конфигурационные файлы
- [ ] Покрытие тестами > 70%
- [ ] Время загрузки страниц < 2 секунд

### Долгосрочные (6 месяцев)
- [ ] Поддержка новых маркетплейсов через плагины
- [ ] Асинхронная обработка больших файлов
- [ ] Мониторинг производительности
- [ ] Автоматическое тестирование и деплой

## Заключение

Данный план улучшений поможет превратить хороший проект в отличный, готовый к продакшену инструмент. Рекомендуется начать с критических исправлений и постепенно двигаться к более сложным архитектурным изменениям. 