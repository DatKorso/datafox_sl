"""
Database migration utilities for handling schema changes.
"""
import duckdb
import streamlit as st
from typing import List


def migrate_integer_to_bigint_tables(conn: duckdb.DuckDBPyConnection) -> bool:
    """
    Migrate tables that have INTEGER columns changed to BIGINT.
    This drops and recreates affected tables.
    
    Tables affected:
    - oz_orders (oz_sku: INTEGER → BIGINT)  
    - oz_products (oz_product_id, oz_sku: INTEGER → BIGINT)
    - oz_barcodes (oz_product_id: INTEGER → BIGINT)
    
    Returns:
        bool: True if migration successful, False otherwise
    """
    affected_tables = ['oz_orders', 'oz_products', 'oz_barcodes']
    
    try:
        # Check which tables exist
        existing_tables = []
        for table_name in affected_tables:
            res = conn.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table_name}';").fetchone()
            if res:
                existing_tables.append(table_name)
        
        if not existing_tables:
            st.info("Миграция не требуется - таблицы не найдены или уже обновлены.")
            return True
        
        st.warning(f"🔄 Выполняется миграция схемы БД. Затронутые таблицы: {', '.join(existing_tables)}")
        st.info("⚠️ Данные в этих таблицах будут удалены. Переимпортируйте данные после миграции.")
        
        # Drop existing tables
        for table_name in existing_tables:
            st.info(f"Удаление таблицы '{table_name}'...")
            conn.execute(f"DROP TABLE IF EXISTS {table_name};")

        # После разрушающих изменений попробуем пересоздать индексы (безопасно: пропустит, если таблиц нет)
        try:
            from .db_indexing import create_performance_indexes
            create_performance_indexes(conn, priority_levels=[1, 2, 3])
        except Exception:
            pass
        
        st.success("✅ Миграция схемы завершена. Таблицы пересозданы с новыми типами данных.")
        st.info("📥 Теперь вы можете переимпортировать данные на странице 'Import Reports'.")
        
        return True
        
    except Exception as e:
        st.error(f"❌ Ошибка при выполнении миграции: {e}")
        return False


def check_migration_needed(conn: duckdb.DuckDBPyConnection) -> bool:
    """
    Check if migration from INTEGER to BIGINT is needed.
    
    Returns:
        bool: True if migration is needed, False otherwise
    """
    tables_to_check = {
        'oz_orders': ['oz_sku'],
        'oz_products': ['oz_product_id', 'oz_sku'], 
        'oz_barcodes': ['oz_product_id']
    }
    
    try:
        for table_name, columns in tables_to_check.items():
            # Check if table exists
            res = conn.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table_name}';").fetchone()
            if not res:
                continue  # Table doesn't exist, no migration needed for this table
            
            # Check column types
            for column_name in columns:
                try:
                    column_info = conn.execute(f"""
                        SELECT data_type 
                        FROM information_schema.columns 
                        WHERE table_name = '{table_name}' AND column_name = '{column_name}';
                    """).fetchone()
                    
                    if column_info and column_info[0] == 'INTEGER':
                        return True  # Found INTEGER column that should be BIGINT
                        
                except Exception:
                    continue  # Column doesn't exist or other issue
        
        return False  # No migration needed
        
    except Exception as e:
        st.error(f"Ошибка при проверке необходимости миграции: {e}")
        return False


def migrate_wb_sku_to_bigint(conn: duckdb.DuckDBPyConnection) -> bool:
    """
    Migrate Wildberries tables that have wb_sku as INTEGER to BIGINT.
    This drops and recreates affected tables.
    
    Tables affected:
    - wb_products (wb_sku: INTEGER → BIGINT)
    - wb_prices (wb_sku: INTEGER → BIGINT)
    
    Returns:
        bool: True if migration successful, False otherwise
    """
    affected_tables = ['wb_products', 'wb_prices']
    
    try:
        # Check which tables exist
        existing_tables = []
        for table_name in affected_tables:
            res = conn.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table_name}';").fetchone()
            if res:
                existing_tables.append(table_name)
        
        if not existing_tables:
            st.info("Миграция WB SKU не требуется - таблицы не найдены или уже обновлены.")
            return True
        
        st.warning(f"🔄 Выполняется миграция схемы БД для WB SKU. Затронутые таблицы: {', '.join(existing_tables)}")
        st.info("⚠️ Данные в этих таблицах будут удалены. Переимпортируйте данные после миграции.")
        
        # Drop existing tables
        for table_name in existing_tables:
            st.info(f"Удаление таблицы '{table_name}'...")
            conn.execute(f"DROP TABLE IF EXISTS {table_name};")

        # После разрушающих изменений попробуем пересоздать индексы (безопасно: пропустит, если таблиц нет)
        try:
            from .db_indexing import create_performance_indexes
            create_performance_indexes(conn, priority_levels=[1, 2, 3])
        except Exception:
            pass
        
        st.success("✅ Миграция WB SKU завершена. Таблицы пересозданы с типом BIGINT для wb_sku.")
        st.info("📥 Теперь вы можете переимпортировать данные Wildberries на странице 'Import Reports'.")
        
        return True
        
    except Exception as e:
        st.error(f"❌ Ошибка при выполнении миграции WB SKU: {e}")
        return False


def check_wb_sku_migration_needed(conn: duckdb.DuckDBPyConnection) -> bool:
    """
    Check if migration from INTEGER to BIGINT is needed for wb_sku fields.
    
    Returns:
        bool: True if migration is needed, False otherwise
    """
    tables_to_check = {
        'wb_products': ['wb_sku'],
        'wb_prices': ['wb_sku']
    }
    
    try:
        for table_name, columns in tables_to_check.items():
            # Check if table exists
            res = conn.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table_name}';").fetchone()
            if not res:
                continue  # Table doesn't exist, no migration needed for this table
            
            # Check column types
            for column_name in columns:
                try:
                    column_info = conn.execute(f"""
                        SELECT data_type 
                        FROM information_schema.columns 
                        WHERE table_name = '{table_name}' AND column_name = '{column_name}';
                    """).fetchone()
                    
                    if column_info and column_info[0] == 'INTEGER':
                        return True  # Found INTEGER column that should be BIGINT
                        
                except Exception:
                    continue  # Column doesn't exist or other issue
        
        return False  # No migration needed
        
    except Exception as e:
        st.error(f"Ошибка при проверке необходимости миграции WB SKU: {e}")
        return False


def auto_migrate_if_needed(conn: duckdb.DuckDBPyConnection) -> bool:
    """
    Automatically check and perform migration if needed.
    
    Returns:
        bool: True if no migration was needed or migration was successful
    """
    try:
        migration_needed = check_migration_needed(conn)
        wb_migration_needed = check_wb_sku_migration_needed(conn)
        
        if migration_needed:
            st.warning("🔄 Обнаружены устаревшие типы данных в схеме БД (Ozon). Требуется миграция.")
            
            with st.expander("ℹ️ Подробности миграции Ozon"):
                st.markdown("""
                **Что изменилось:**
                - `oz_orders.oz_sku`: INTEGER → BIGINT
                - `oz_products.oz_product_id`: INTEGER → BIGINT  
                - `oz_products.oz_sku`: INTEGER → BIGINT
                - `oz_barcodes.oz_product_id`: INTEGER → BIGINT
                
                **Причина изменения:**
                Значения SKU в новых отчетах Ozon превышают максимальное значение INT32 (2,147,483,647).
                
                **Действие:**
                Таблицы будут пересозданы с типом BIGINT для поддержки больших значений.
                """)
            
            if st.button("🚀 Выполнить миграцию Ozon", type="primary", key="migrate_ozon"):
                if migrate_integer_to_bigint_tables(conn):
                    st.rerun()  # Refresh page after successful migration
                return False
        
        if wb_migration_needed:
            st.warning("🔄 Обнаружены устаревшие типы данных в схеме БД (Wildberries). Требуется миграция.")
            
            with st.expander("ℹ️ Подробности миграции Wildberries"):
                st.markdown("""
                **Что изменилось:**
                - `wb_products.wb_sku`: INTEGER → BIGINT
                - `wb_prices.wb_sku`: INTEGER → BIGINT
                
                **Причина изменения:**
                Значения SKU в отчетах Wildberries могут превышать максимальное значение INT32 (2,147,483,647).
                
                **Действие:**
                Таблицы будут пересозданы с типом BIGINT для поддержки больших значений.
                """)
            
            if st.button("🚀 Выполнить миграцию Wildberries", type="primary", key="migrate_wb"):
                if migrate_wb_sku_to_bigint(conn):
                    st.rerun()  # Refresh page after successful migration
                return False
        
        if migration_needed or wb_migration_needed:
            st.info("Нажмите соответствующие кнопки выше для выполнения миграций.")
            return False
        
        return True  # No migration needed
        
    except Exception as e:
        st.error(f"❌ Ошибка при проверке необходимости миграции: {e}")
        return False 
