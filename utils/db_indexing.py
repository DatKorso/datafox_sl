"""
Модуль управления индексами базы данных DataFox SL.

Основные функции:
- Создание критически важных индексов для оптимизации производительности
- Автоматическое пересоздание индексов после обновления данных
- Проверка существования и статуса индексов
- Обработка ошибок без прерывания основных процессов

Автор: DataFox SL Project
Версия: 1.0.0
"""

import duckdb
import streamlit as st
import logging
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass

# Настройка логирования
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


@dataclass
class IndexDefinition:
    """Определение индекса для создания в базе данных"""
    name: str
    table: str
    columns: List[str]
    unique: bool = False
    priority: int = 1  # 1=критический, 2=важный, 3=дополнительный
    description: str = ""


# Определение всех индексов проекта
PERFORMANCE_INDEXES = [
    # ПРИОРИТЕТ 1: Критически важные индексы
    IndexDefinition(
        name="idx_oz_barcodes_barcode",
        table="oz_barcodes",
        columns=["oz_barcode"],
        priority=1,
        description="Критический для cross-marketplace linking через штрихкоды"
    ),
    IndexDefinition(
        name="idx_wb_products_sku",
        table="wb_products", 
        columns=["wb_sku"],
        priority=1,
        description="Основной поиск WB товаров по SKU"
    ),
    IndexDefinition(
        name="idx_oz_products_vendor_code",
        table="oz_products",
        columns=["oz_vendor_code"],
        priority=1,
        description="Частые запросы по vendor code в oz_products"
    ),
    IndexDefinition(
        name="idx_oz_category_products_vendor_code", 
        table="oz_category_products",
        columns=["oz_vendor_code"],
        priority=1,
        description="Аналитические запросы по vendor code"
    ),
    
    # ПРИОРИТЕТ 2: Важные индексы
    IndexDefinition(
        name="idx_oz_products_sku",
        table="oz_products",
        columns=["oz_sku"],
        priority=2,
        description="Поиск Ozon товаров по SKU"
    ),
    IndexDefinition(
        name="idx_oz_barcodes_product_id",
        table="oz_barcodes",
        columns=["oz_product_id"], 
        priority=2,
        description="Связывание штрихкодов с товарами через product_id"
    ),
    IndexDefinition(
        name="idx_wb_products_brand",
        table="wb_products",
        columns=["wb_brand"],
        priority=2,
        description="Фильтрация WB товаров по бренду в рекомендациях"
    ),
    IndexDefinition(
        name="idx_oz_products_brand",
        table="oz_products",
        columns=["oz_brand"],
        priority=2,
        description="Фильтрация Ozon товаров по бренду"
    ),
    
    # ПРИОРИТЕТ 3: Композитные индексы
    IndexDefinition(
        name="idx_oz_products_composite",
        table="oz_products",
        columns=["oz_vendor_code", "oz_sku"],
        priority=3,
        description="Композитный индекс для сложных cross-marketplace запросов"
    ),
    IndexDefinition(
        name="idx_oz_products_stock_status",
        table="oz_products",
        columns=["oz_fbo_stock", "oz_product_status"],
        priority=3,
        description="Фильтрация по наличию и статусу товара"
    )
]


def get_indexes_by_priority(priority: int) -> List[IndexDefinition]:
    """Возвращает индексы указанного приоритета"""
    return [idx for idx in PERFORMANCE_INDEXES if idx.priority == priority]


def check_index_exists(conn: duckdb.DuckDBPyConnection, index_name: str) -> bool:
    """
    Проверяет существование индекса в базе данных.
    
    Args:
        conn: Соединение с базой данных
        index_name: Имя индекса для проверки
        
    Returns:
        True если индекс существует, False иначе
    """
    try:
        result = conn.execute("""
            SELECT COUNT(*) as count 
            FROM duckdb_indexes() 
            WHERE index_name = ?
        """, [index_name]).fetchone()
        
        return result[0] > 0 if result else False
        
    except Exception as e:
        logger.warning(f"Ошибка проверки индекса {index_name}: {e}")
        return False


def create_single_index(conn: duckdb.DuckDBPyConnection, index_def: IndexDefinition) -> Tuple[bool, str]:
    """
    Создает один индекс в базе данных.
    
    Args:
        conn: Соединение с базой данных
        index_def: Определение индекса для создания
        
    Returns:
        Tuple[success: bool, message: str]
    """
    try:
        # Проверяем существование таблицы
        table_check = conn.execute("""
            SELECT COUNT(*) as count 
            FROM information_schema.tables 
            WHERE table_name = ?
        """, [index_def.table]).fetchone()
        
        if not table_check or table_check[0] == 0:
            return False, f"Таблица {index_def.table} не существует"
        
        # Формируем SQL для создания индекса
        columns_sql = ", ".join(index_def.columns)
        unique_keyword = "UNIQUE " if index_def.unique else ""
        
        create_sql = f"""
            CREATE {unique_keyword}INDEX IF NOT EXISTS {index_def.name} 
            ON {index_def.table}({columns_sql})
        """
        
        # Создаем индекс
        conn.execute(create_sql)
        
        success_msg = f"✅ Индекс {index_def.name} создан успешно"
        logger.info(success_msg)
        
        return True, success_msg
        
    except Exception as e:
        error_msg = f"❌ Ошибка создания индекса {index_def.name}: {e}"
        logger.error(error_msg)
        return False, error_msg


def create_performance_indexes(
    conn: duckdb.DuckDBPyConnection, 
    priority_levels: Optional[List[int]] = None,
    force_recreate: bool = False
) -> Dict[str, Tuple[bool, str]]:
    """
    Создает индексы для оптимизации производительности.
    
    Args:
        conn: Соединение с базой данных
        priority_levels: Список приоритетов для создания (по умолчанию [1, 2])
        force_recreate: Принудительно пересоздать существующие индексы
        
    Returns:
        Словарь {имя_индекса: (успех, сообщение)}
    """
    if not conn:
        return {"error": (False, "Нет соединения с базой данных")}
    
    if priority_levels is None:
        priority_levels = [1, 2]  # По умолчанию создаем критические и важные
    
    results = {}
    
    try:
        # Получаем индексы для создания
        indexes_to_create = []
        for priority in priority_levels:
            indexes_to_create.extend(get_indexes_by_priority(priority))
        
        logger.info(f"Начинаем создание {len(indexes_to_create)} индексов...")
        
        for index_def in indexes_to_create:
            # Проверяем существование индекса
            if not force_recreate and check_index_exists(conn, index_def.name):
                results[index_def.name] = (True, f"Индекс {index_def.name} уже существует")
                continue
            
            # Если нужно пересоздать, сначала удаляем
            if force_recreate:
                try:
                    conn.execute(f"DROP INDEX IF EXISTS {index_def.name}")
                    logger.info(f"Удален существующий индекс {index_def.name}")
                except Exception as e:
                    logger.warning(f"Ошибка удаления индекса {index_def.name}: {e}")
            
            # Создаем индекс
            success, message = create_single_index(conn, index_def)
            results[index_def.name] = (success, message)
        
        # Подсчитываем статистику
        successful = sum(1 for success, _ in results.values() if success)
        total = len(results)
        
        summary_msg = f"Создано индексов: {successful}/{total}"
        logger.info(summary_msg)
        
        # Добавляем summary в результаты
        results["_summary"] = (True, summary_msg)
        
        return results
        
    except Exception as e:
        error_msg = f"Критическая ошибка создания индексов: {e}"
        logger.error(error_msg)
        return {"error": (False, error_msg)}


def get_indexes_status(conn: duckdb.DuckDBPyConnection) -> Dict[str, Dict]:
    """
    Получает статус всех индексов проекта.
    
    Args:
        conn: Соединение с базой данных
        
    Returns:
        Словарь со статусом каждого индекса
    """
    if not conn:
        return {"error": "Нет соединения с базой данных"}
    
    status = {}
    
    try:
        for index_def in PERFORMANCE_INDEXES:
            exists = check_index_exists(conn, index_def.name)
            
            status[index_def.name] = {
                "exists": exists,
                "table": index_def.table,
                "columns": index_def.columns,
                "priority": index_def.priority,
                "description": index_def.description
            }
        
        return status
        
    except Exception as e:
        logger.error(f"Ошибка получения статуса индексов: {e}")
        return {"error": str(e)}


def recreate_indexes_after_import(
    conn: duckdb.DuckDBPyConnection, 
    table_name: str,
    silent: bool = False
) -> bool:
    """
    Пересоздает индексы для конкретной таблицы после импорта данных.
    
    Args:
        conn: Соединение с базой данных
        table_name: Имя таблицы, для которой нужно пересоздать индексы
        silent: Если True, не выводит сообщения в Streamlit
        
    Returns:
        True если все индексы созданы успешно
    """
    if not conn:
        if not silent:
            st.warning("Нет соединения с базой данных для создания индексов")
        return False
    
    # Находим индексы для данной таблицы
    table_indexes = [idx for idx in PERFORMANCE_INDEXES if idx.table == table_name]
    
    if not table_indexes:
        logger.info(f"Нет индексов для таблицы {table_name}")
        return True
    
    success_count = 0
    
    try:
        for index_def in table_indexes:
            success, message = create_single_index(conn, index_def)
            
            if success:
                success_count += 1
                if not silent:
                    st.success(message)
            else:
                if not silent:
                    st.warning(message)
                logger.warning(message)
        
        all_successful = success_count == len(table_indexes)
        
        if not silent and table_indexes:
            if all_successful:
                st.info(f"🔍 Созданы все индексы для таблицы {table_name} ({success_count}/{len(table_indexes)})")
            else:
                st.warning(f"⚠️ Частично созданы индексы для таблицы {table_name} ({success_count}/{len(table_indexes)})")
        
        return all_successful
        
    except Exception as e:
        error_msg = f"Ошибка пересоздания индексов для таблицы {table_name}: {e}"
        logger.error(error_msg)
        if not silent:
            st.error(error_msg)
        return False


def drop_all_performance_indexes(conn: duckdb.DuckDBPyConnection) -> Dict[str, Tuple[bool, str]]:
    """
    Удаляет все индексы производительности (для целей отладки/очистки).
    
    Args:
        conn: Соединение с базой данных
        
    Returns:
        Словарь результатов удаления
    """
    if not conn:
        return {"error": (False, "Нет соединения с базой данных")}
    
    results = {}
    
    try:
        for index_def in PERFORMANCE_INDEXES:
            try:
                conn.execute(f"DROP INDEX IF EXISTS {index_def.name}")
                results[index_def.name] = (True, f"✅ Индекс {index_def.name} удален")
                logger.info(f"Удален индекс {index_def.name}")
            except Exception as e:
                results[index_def.name] = (False, f"❌ Ошибка удаления {index_def.name}: {e}")
                logger.error(f"Ошибка удаления индекса {index_def.name}: {e}")
        
        successful = sum(1 for success, _ in results.values() if success)
        total = len(results)
        
        results["_summary"] = (True, f"Удалено индексов: {successful}/{total}")
        
        return results
        
    except Exception as e:
        error_msg = f"Критическая ошибка удаления индексов: {e}"
        logger.error(error_msg)
        return {"error": (False, error_msg)}


# Функции для интеграции с существующими модулями
def ensure_critical_indexes(conn: duckdb.DuckDBPyConnection) -> bool:
    """
    Убеждается что критические индексы (приоритет 1) существуют.
    Используется при инициализации схемы БД.
    
    Args:
        conn: Соединение с базой данных
        
    Returns:
        True если все критические индексы созданы
    """
    if not conn:
        return False
    
    try:
        results = create_performance_indexes(conn, priority_levels=[1])
        
        # Проверяем что все критические индексы созданы успешно
        critical_success = all(
            success for name, (success, _) in results.items() 
            if name != "_summary" and name != "error"
        )
        
        return critical_success
        
    except Exception as e:
        logger.error(f"Ошибка создания критических индексов: {e}")
        return False