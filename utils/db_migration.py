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


def auto_migrate_if_needed(conn: duckdb.DuckDBPyConnection) -> bool:
    """
    Automatically check and perform migration if needed.
    
    Returns:
        bool: True if no migration was needed or migration was successful
    """
    if check_migration_needed(conn):
        st.warning("🔄 Обнаружены устаревшие типы данных в схеме БД. Требуется миграция.")
        
        with st.expander("ℹ️ Подробности миграции"):
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
        
        if st.button("🚀 Выполнить миграцию", type="primary"):
            return migrate_integer_to_bigint_tables(conn)
        else:
            st.info("Нажмите кнопку выше для выполнения миграции.")
            return False
    
    return True  # No migration needed 