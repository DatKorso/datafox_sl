# 📊 Google Sheets Integration

> Техническая документация интеграции DataFox SL с Google Sheets API

## 🎯 Обзор интеграции

Google Sheets интеграция в DataFox SL обеспечивает автоматическое получение справочных данных (Punta table) из Google Sheets документов. Система поддерживает автоматическую аутентификацию, кэширование данных и динамическое обновление схемы базы данных.

## 🏗️ Архитектура интеграции

### Основные компоненты

#### **1. Аутентификация и подключение**
```python
# google_sheets_utils.py - Основной модуль интеграции
def authenticate_google_sheets(credentials_json_path: str) -> gspread.Client:
    """
    Аутентификация в Google Sheets API через service account
    
    Args:
        credentials_json_path: Путь к JSON файлу с ключом service account
    
    Returns:
        Авторизованный клиент gspread
    """
```

#### **2. Загрузчик данных**
```python
def load_google_sheet_data(
    credentials_json_path: str,
    spreadsheet_id: str,
    sheet_name: str = None,
    header_row: int = 1
) -> pd.DataFrame:
    """
    Загружает данные из Google Sheets документа
    
    Поддерживает:
    - Автоматическое определение листа
    - Настраиваемая строка заголовков
    - Обработку ошибок сети
    - Валидацию данных
    """
```

#### **3. Система кэширования**
```python
def cache_google_sheets_data(df: pd.DataFrame, cache_file_path: str) -> bool:
    """
    Кэширует данные Google Sheets в локальный файл
    
    Форматы кэша:
    - Pickle для быстрой загрузки
    - CSV для резервного доступа
    - JSON для метаданных
    """
```

### Поток интеграции

Процесс интеграции включает следующие этапы:

1. **Проверка конфигурации** Google Sheets
2. **Аутентификация** через service account
3. **Подключение к документу** по spreadsheet_id
4. **Загрузка данных** с указанного листа
5. **Валидация и очистка** полученных данных
6. **Кэширование** для оффлайн доступа
7. **Обновление схемы БД** при необходимости
8. **Импорт в DuckDB** через стандартный механизм

## 🔧 Техническая реализация

### Аутентификация Google Sheets API

#### **Service Account аутентификация**
```python
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import streamlit as st
import os
import pickle
import json
from datetime import datetime, timedelta

def authenticate_google_sheets(credentials_json_path: str) -> gspread.Client:
    """
    Создает аутентифицированного клиента Google Sheets API
    
    Требуемые разрешения:
    - https://www.googleapis.com/auth/spreadsheets.readonly
    - https://www.googleapis.com/auth/drive.readonly
    """
    try:
        if not os.path.exists(credentials_json_path):
            raise FileNotFoundError(f"Файл credentials не найден: {credentials_json_path}")
        
        # Определение скоупов для доступа
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets.readonly',
            'https://www.googleapis.com/auth/drive.readonly'
        ]
        
        # Создание credentials объекта
        credentials = Credentials.from_service_account_file(
            credentials_json_path, 
            scopes=scopes
        )
        
        # Создание клиента
        client = gspread.authorize(credentials)
        
        return client
        
    except FileNotFoundError as e:
        st.error(f"Ошибка: {e}")
        return None
    except Exception as e:
        st.error(f"Ошибка аутентификации Google Sheets: {e}")
        return None
```

#### **Проверка доступа**
```python
def test_google_sheets_access(client: gspread.Client, spreadsheet_id: str) -> bool:
    """
    Тестирует доступ к конкретному Google Sheets документу
    """
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        sheet_names = [sheet.title for sheet in spreadsheet.worksheets()]
        
        st.success(f"✅ Доступ к документу подтвержден. Найдено листов: {len(sheet_names)}")
        st.info(f"Доступные листы: {', '.join(sheet_names)}")
        
        return True
        
    except gspread.exceptions.SpreadsheetNotFound:
        st.error("❌ Документ не найден или нет доступа")
        return False
    except gspread.exceptions.APIError as e:
        st.error(f"❌ Ошибка API Google Sheets: {e}")
        return False
    except Exception as e:
        st.error(f"❌ Неожиданная ошибка: {e}")
        return False
```

### Загрузка и обработка данных

#### **Основная функция загрузки**
```python
def load_google_sheet_data(
    credentials_json_path: str,
    spreadsheet_id: str,
    sheet_name: str = None,
    header_row: int = 1,
    use_cache: bool = True,
    cache_duration_hours: int = 24
) -> pd.DataFrame:
    """
    Загружает данные из Google Sheets с поддержкой кэширования
    """
    try:
        # Проверка кэша
        if use_cache:
            cached_data = load_from_cache(spreadsheet_id, sheet_name, cache_duration_hours)
            if cached_data is not None:
                st.info("📁 Данные загружены из кэша")
                return cached_data
        
        # Аутентификация
        client = authenticate_google_sheets(credentials_json_path)
        if not client:
            return pd.DataFrame()
        
        # Открытие документа
        spreadsheet = client.open_by_key(spreadsheet_id)
        
        # Выбор листа
        if sheet_name:
            worksheet = spreadsheet.worksheet(sheet_name)
        else:
            worksheet = spreadsheet.sheet1  # Первый лист по умолчанию
        
        # Получение всех данных
        all_values = worksheet.get_all_values()
        
        if not all_values:
            st.warning("⚠️ Google Sheets документ пуст")
            return pd.DataFrame()
        
        # Создание DataFrame
        if len(all_values) < header_row:
            st.error(f"❌ Недостаточно строк. Заголовки ожидаются в строке {header_row}")
            return pd.DataFrame()
        
        headers = all_values[header_row - 1]  # Строка заголовков (индексация с 0)
        data_rows = all_values[header_row:]   # Строки данных
        
        df = pd.DataFrame(data_rows, columns=headers)
        
        # Очистка данных
        df = clean_google_sheets_data(df)
        
        # Кэширование
        if use_cache and not df.empty:
            cache_google_sheets_data(df, spreadsheet_id, sheet_name)
        
        st.success(f"✅ Загружено {len(df)} строк из Google Sheets")
        return df
        
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"❌ Лист '{sheet_name}' не найден в документе")
        return pd.DataFrame()
    except gspread.exceptions.APIError as e:
        st.error(f"❌ Ошибка API Google Sheets: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Ошибка загрузки данных: {e}")
        return pd.DataFrame()
```

#### **Очистка данных**
```python
def clean_google_sheets_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Очищает и стандартизирует данные из Google Sheets
    """
    if df.empty:
        return df
    
    try:
        # Удаление полностью пустых строк
        df = df.dropna(how='all')
        
        # Удаление полностью пустых колонок
        df = df.dropna(axis=1, how='all')
        
        # Очистка названий колонок
        df.columns = [
            str(col).strip().replace('\n', ' ').replace('\r', '')
            for col in df.columns
        ]
        
        # Удаление дублирующихся колонок
        df = df.loc[:, ~df.columns.duplicated()]
        
        # Обрезка пробелов в строковых данных
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].astype(str).str.strip()
        
        # Замена пустых строк на None
        df = df.replace('', None)
        
        return df
        
    except Exception as e:
        st.warning(f"⚠️ Ошибка очистки данных: {e}")
        return df
```

### Система кэширования

#### **Сохранение в кэш**
```python
def cache_google_sheets_data(df: pd.DataFrame, spreadsheet_id: str, sheet_name: str = None) -> bool:
    """
    Сохраняет данные Google Sheets в локальный кэш
    """
    try:
        # Создание директории кэша
        cache_dir = os.path.join(os.getcwd(), '.google_sheets_cache')
        os.makedirs(cache_dir, exist_ok=True)
        
        # Формирование имени файла кэша
        sheet_suffix = f"_{sheet_name}" if sheet_name else ""
        cache_file_base = f"{spreadsheet_id}{sheet_suffix}"
        
        # Сохранение данных в pickle
        pickle_path = os.path.join(cache_dir, f"{cache_file_base}.pkl")
        with open(pickle_path, 'wb') as f:
            pickle.dump(df, f)
        
        # Сохранение метаданных
        metadata = {
            'spreadsheet_id': spreadsheet_id,
            'sheet_name': sheet_name,
            'cached_at': datetime.now().isoformat(),
            'row_count': len(df),
            'column_count': len(df.columns),
            'columns': list(df.columns)
        }
        
        metadata_path = os.path.join(cache_dir, f"{cache_file_base}_metadata.json")
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        # Сохранение CSV резервной копии
        csv_path = os.path.join(cache_dir, f"{cache_file_base}.csv")
        df.to_csv(csv_path, index=False, encoding='utf-8')
        
        return True
        
    except Exception as e:
        st.warning(f"⚠️ Ошибка кэширования: {e}")
        return False
```

#### **Загрузка из кэша**
```python
def load_from_cache(spreadsheet_id: str, sheet_name: str = None, max_age_hours: int = 24) -> pd.DataFrame:
    """
    Загружает данные из кэша, если они не устарели
    """
    try:
        cache_dir = os.path.join(os.getcwd(), '.google_sheets_cache')
        if not os.path.exists(cache_dir):
            return None
        
        # Формирование имени файла
        sheet_suffix = f"_{sheet_name}" if sheet_name else ""
        cache_file_base = f"{spreadsheet_id}{sheet_suffix}"
        
        pickle_path = os.path.join(cache_dir, f"{cache_file_base}.pkl")
        metadata_path = os.path.join(cache_dir, f"{cache_file_base}_metadata.json")
        
        if not os.path.exists(pickle_path) or not os.path.exists(metadata_path):
            return None
        
        # Проверка возраста кэша
        with open(metadata_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        cached_at = datetime.fromisoformat(metadata['cached_at'])
        age_hours = (datetime.now() - cached_at).total_seconds() / 3600
        
        if age_hours > max_age_hours:
            return None
        
        # Загрузка данных
        with open(pickle_path, 'rb') as f:
            df = pickle.load(f)
        
        return df
        
    except Exception as e:
        # Возвращаем None при любой ошибке, чтобы форсировать загрузку из API
        return None
```

#### **Управление кэшем**
```python
def clear_google_sheets_cache(spreadsheet_id: str = None) -> bool:
    """
    Очищает кэш Google Sheets
    
    Args:
        spreadsheet_id: ID конкретного документа для очистки.
                       Если None, очищает весь кэш.
    """
    try:
        cache_dir = os.path.join(os.getcwd(), '.google_sheets_cache')
        if not os.path.exists(cache_dir):
            return True
        
        if spreadsheet_id:
            # Очистка кэша конкретного документа
            for filename in os.listdir(cache_dir):
                if filename.startswith(spreadsheet_id):
                    file_path = os.path.join(cache_dir, filename)
                    os.remove(file_path)
        else:
            # Очистка всего кэша
            import shutil
            shutil.rmtree(cache_dir)
        
        return True
        
    except Exception as e:
        st.error(f"Ошибка очистки кэша: {e}")
        return False

def get_cache_info() -> dict:
    """
    Возвращает информацию о состоянии кэша Google Sheets
    """
    try:
        cache_dir = os.path.join(os.getcwd(), '.google_sheets_cache')
        if not os.path.exists(cache_dir):
            return {'cache_exists': False}
        
        cache_files = [f for f in os.listdir(cache_dir) if f.endswith('.pkl')]
        total_size = sum(
            os.path.getsize(os.path.join(cache_dir, f))
            for f in os.listdir(cache_dir)
        )
        
        return {
            'cache_exists': True,
            'cached_documents': len(cache_files),
            'total_size_mb': round(total_size / 1024 / 1024, 2),
            'cache_dir': cache_dir
        }
        
    except Exception as e:
        return {'cache_exists': False, 'error': str(e)}
```

### Интеграция с базой данных

#### **Автоматический импорт в DuckDB**
```python
def import_google_sheets_to_db(
    db_conn,
    credentials_json_path: str,
    spreadsheet_id: str,
    sheet_name: str = None,
    table_name: str = 'punta_table',
    update_mode: str = 'replace'
) -> tuple[bool, str]:
    """
    Импортирует данные Google Sheets напрямую в DuckDB
    
    Args:
        update_mode: 'replace', 'append', или 'update'
    """
    try:
        # Загрузка данных из Google Sheets
        df = load_google_sheet_data(
            credentials_json_path,
            spreadsheet_id, 
            sheet_name
        )
        
        if df.empty:
            return False, "Нет данных для импорта"
        
        # Добавление системных колонок
        df['_source_file'] = f"google_sheets:{spreadsheet_id}"
        df['_import_date'] = datetime.now()
        
        # Выполнение импорта в зависимости от режима
        if update_mode == 'replace':
            # Полная замена таблицы
            db_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            db_conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            
        elif update_mode == 'append':
            # Добавление новых данных
            db_conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
            
        elif update_mode == 'update':
            # Обновление существующих данных
            # Сначала удаляем старые записи с тем же _source_file
            db_conn.execute(f"""
                DELETE FROM {table_name} 
                WHERE _source_file = ?
            """, [f"google_sheets:{spreadsheet_id}"])
            
            # Затем вставляем новые данные
            db_conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
        
        row_count = len(df)
        return True, f"Импортировано {row_count} строк в таблицу {table_name}"
        
    except Exception as e:
        return False, f"Ошибка импорта в БД: {e}"
```

#### **Динамическое обновление схемы**
```python
def update_table_schema_for_google_sheets(db_conn, df: pd.DataFrame, table_name: str) -> bool:
    """
    Обновляет схему таблицы при изменении структуры Google Sheets
    """
    try:
        # Получение текущей схемы таблицы
        try:
            current_schema = db_conn.execute(f"DESCRIBE {table_name}").fetchdf()
            current_columns = set(current_schema['column_name'].tolist())
        except:
            # Таблица не существует
            current_columns = set()
        
        # Анализ новых колонок
        new_columns = set(df.columns)
        added_columns = new_columns - current_columns
        removed_columns = current_columns - new_columns
        
        if added_columns:
            st.info(f"🆕 Обнаружены новые колонки: {', '.join(added_columns)}")
            
            # Добавление новых колонок
            for col in added_columns:
                try:
                    db_conn.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" VARCHAR')
                except:
                    pass  # Колонка уже может существовать
        
        if removed_columns:
            st.warning(f"⚠️ Колонки удалены из источника: {', '.join(removed_columns)}")
        
        return True
        
    except Exception as e:
        st.error(f"Ошибка обновления схемы таблицы: {e}")
        return False
```

## 🔧 Конфигурация и настройка

### Настройка Service Account

#### **Создание Service Account в Google Cloud**
```markdown
1. Перейти в Google Cloud Console
2. Создать новый проект или выбрать существующий
3. Включить Google Sheets API и Google Drive API
4. Создать Service Account:
   - IAM & Admin > Service Accounts
   - Создать Service Account
   - Скачать JSON ключ
5. Предоставить доступ к документу:
   - Открыть Google Sheets документ
   - Нажать "Поделиться"
   - Добавить email Service Account с правами "Просмотр"
```

#### **Структура credentials.json**
```json
{
  "type": "service_account",
  "project_id": "your-project-id",
  "private_key_id": "key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
  "client_email": "your-service-account@your-project.iam.gserviceaccount.com",
  "client_id": "client-id",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/your-service-account%40your-project.iam.gserviceaccount.com"
}
```

### Конфигурация в DataFox SL

#### **Настройки Google Sheets в config.json**
```json
{
  "google_sheets": {
    "enabled": true,
    "credentials_path": "path/to/service-account-key.json",
    "default_spreadsheet_id": "1234567890abcdef",
    "default_sheet_name": "Данные",
    "cache_enabled": true,
    "cache_duration_hours": 24,
    "auto_refresh": true,
    "refresh_interval_hours": 6
  }
}
```

#### **Использование в Streamlit интерфейсе**
```python
# В pages/3_⚙️_Настройки.py
def configure_google_sheets():
    """
    Интерфейс настройки Google Sheets интеграции
    """
    st.subheader("🔗 Google Sheets Integration")
    
    # Загрузка credentials файла
    credentials_file = st.file_uploader(
        "Загрузить Service Account JSON",
        type=['json'],
        help="Скачайте JSON ключ Service Account из Google Cloud Console"
    )
    
    if credentials_file:
        # Сохранение файла
        credentials_path = save_uploaded_credentials(credentials_file)
        st.success("✅ Credentials файл загружен")
        
        # Тестирование подключения
        spreadsheet_id = st.text_input(
            "ID Google Sheets документа",
            help="Найдите ID в URL документа"
        )
        
        if spreadsheet_id:
            if st.button("🔍 Тестировать подключение"):
                client = authenticate_google_sheets(credentials_path)
                if client:
                    success = test_google_sheets_access(client, spreadsheet_id)
                    if success:
                        # Сохранение конфигурации
                        save_google_sheets_config(credentials_path, spreadsheet_id)
```

## 📊 Мониторинг и диагностика

### Статистика использования

#### **Мониторинг API вызовов**
```python
def track_api_usage() -> dict:
    """
    Отслеживает использование Google Sheets API
    """
    try:
        usage_file = os.path.join('.google_sheets_cache', 'api_usage.json')
        
        if os.path.exists(usage_file):
            with open(usage_file, 'r') as f:
                usage_data = json.load(f)
        else:
            usage_data = {
                'daily_requests': {},
                'total_requests': 0,
                'last_request': None
            }
        
        # Обновление статистики
        today = datetime.now().strftime('%Y-%m-%d')
        usage_data['daily_requests'][today] = usage_data['daily_requests'].get(today, 0) + 1
        usage_data['total_requests'] += 1
        usage_data['last_request'] = datetime.now().isoformat()
        
        # Сохранение обновленной статистики
        os.makedirs(os.path.dirname(usage_file), exist_ok=True)
        with open(usage_file, 'w') as f:
            json.dump(usage_data, f, indent=2)
        
        return usage_data
        
    except Exception as e:
        return {'error': str(e)}
```

#### **Диагностика проблем**
```python
def diagnose_google_sheets_issues(credentials_path: str, spreadsheet_id: str) -> dict:
    """
    Комплексная диагностика проблем с Google Sheets интеграцией
    """
    result = {
        'credentials_file': False,
        'authentication': False,
        'spreadsheet_access': False,
        'api_quota': False,
        'data_quality': False,
        'issues': [],
        'recommendations': []
    }
    
    try:
        # Проверка credentials файла
        if os.path.exists(credentials_path):
            result['credentials_file'] = True
            
            try:
                with open(credentials_path, 'r') as f:
                    creds = json.load(f)
                
                required_fields = ['type', 'project_id', 'private_key', 'client_email']
                missing_fields = [field for field in required_fields if field not in creds]
                
                if missing_fields:
                    result['issues'].append(f"Отсутствуют поля в credentials: {missing_fields}")
                else:
                    result['authentication'] = True
                    
            except json.JSONDecodeError:
                result['issues'].append("Невалидный JSON в credentials файле")
        else:
            result['issues'].append(f"Credentials файл не найден: {credentials_path}")
        
        # Проверка подключения к API
        if result['authentication']:
            try:
                client = authenticate_google_sheets(credentials_path)
                if client:
                    result['spreadsheet_access'] = test_google_sheets_access(client, spreadsheet_id)
                    
                    if not result['spreadsheet_access']:
                        result['issues'].append("Нет доступа к указанному документу")
                        result['recommendations'].append("Проверьте права доступа Service Account к документу")
                
            except Exception as e:
                result['issues'].append(f"Ошибка подключения к API: {e}")
        
        # Проверка квот API
        usage_data = track_api_usage()
        today = datetime.now().strftime('%Y-%m-%d')
        daily_requests = usage_data.get('daily_requests', {}).get(today, 0)
        
        if daily_requests > 100:  # Google Sheets имеет лимит ~100 запросов/100 секунд/пользователя
            result['issues'].append(f"Высокое использование API: {daily_requests} запросов сегодня")
            result['recommendations'].append("Увеличьте интервал кэширования")
        else:
            result['api_quota'] = True
        
        return result
        
    except Exception as e:
        result['issues'].append(f"Ошибка диагностики: {e}")
        return result
```

## 🚨 Обработка ошибок

### Типы ошибок интеграции

#### **1. Ошибки аутентификации**
```python
class GoogleSheetsAuthError(Exception):
    """Ошибка аутентификации Google Sheets API"""
    pass

class CredentialsError(Exception):
    """Ошибка с credentials файлом"""
    pass
```

#### **2. Ошибки доступа к данным**
```python
class SpreadsheetAccessError(Exception):
    """Ошибка доступа к Google Sheets документу"""
    pass

class DataLoadError(Exception):
    """Ошибка загрузки данных"""
    pass
```

#### **3. Обработка сетевых ошибок**
```python
def handle_network_errors(func):
    """
    Декоратор для обработки сетевых ошибок при работе с Google Sheets API
    """
    def wrapper(*args, **kwargs):
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
                
            except requests.exceptions.ConnectionError:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                else:
                    st.error("❌ Ошибка сетевого подключения к Google Sheets")
                    return None
                    
            except gspread.exceptions.APIError as e:
                if "RATE_LIMIT_EXCEEDED" in str(e):
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay * (attempt + 1) * 2)  # Увеличенная задержка для лимита
                        continue
                    else:
                        st.error("❌ Превышен лимит запросов к Google Sheets API")
                        return None
                else:
                    st.error(f"❌ Ошибка Google Sheets API: {e}")
                    return None
                    
            except Exception as e:
                st.error(f"❌ Неожиданная ошибка: {e}")
                return None
    
    return wrapper

@handle_network_errors
def robust_load_google_sheet_data(*args, **kwargs):
    """
    Версия load_google_sheet_data с обработкой сетевых ошибок
    """
    return load_google_sheet_data(*args, **kwargs)
```

---

## 📝 Метаданные

**Модуль**: `utils/google_sheets_utils.py`  
**Размер**: 259 строк кода  
**Последнее обновление**: 2024-12-19  
**Версия**: 1.1.0  
**Статус**: Стабильный  

**Зависимости**:
- `gspread` - клиент Google Sheets API
- `google-auth` - аутентификация Google
- `pandas` - обработка данных
- `streamlit` - интерфейс пользователя

**Связанные документы**:
- [Пользовательский гид настроек](../../user-guides/settings.md)
- [Система импорта](import-system.md)
- [Database Utils API](../api/database-utils.md)

*Google Sheets интеграция обеспечивает бесшовную синхронизацию справочных данных с облачными таблицами.* 