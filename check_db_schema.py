#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.db_connection import connect_db

def check_wb_category():
    """Проверяет наличие wb_category в базе данных"""
    conn = connect_db()
    if not conn:
        print("Ошибка подключения к БД")
        return
    
    try:
        # Проверяем существующие таблицы
        tables_query = "SHOW TABLES"
        tables = conn.execute(tables_query).fetchall()
        print("Существующие таблицы:")
        for table in tables:
            print(f"  - {table[0]}")
        
        # Проверяем структуру wb_products если она существует
        wb_tables = [t[0] for t in tables if 'wb' in t[0].lower()]
        if wb_tables:
            print("\nТаблицы WB:")
            for table_name in wb_tables:
                print(f"\n=== {table_name} ===")
                try:
                    columns_query = f"DESCRIBE {table_name}"
                    columns = conn.execute(columns_query).fetchall()
                    for col in columns:
                        print(f"  {col[0]} ({col[1]})")
                        
                    # Проверяем наличие данных
                    count_query = f"SELECT COUNT(*) FROM {table_name}"
                    count = conn.execute(count_query).fetchone()[0]
                    print(f"  Записей: {count}")
                    
                    # Если это wb_products, проверяем wb_category
                    if table_name == 'wb_products':
                        if count > 0:
                            sample_query = f"SELECT * FROM {table_name} LIMIT 3"
                            sample = conn.execute(sample_query).fetchall()
                            print(f"  Примеры данных:")
                            for row in sample:
                                print(f"    {row}")
                except Exception as e:
                    print(f"  Ошибка при анализе таблицы {table_name}: {e}")
        else:
            print("\nТаблицы WB не найдены")
            
        # Проверяем punta_table
        punta_tables = [t[0] for t in tables if 'punta' in t[0].lower()]
        if punta_tables:
            print("\n=== PUNTA TABLES ===")
            for table_name in punta_tables:
                print(f"\n--- {table_name} ---")
                try:
                    columns_query = f"DESCRIBE {table_name}"
                    columns = conn.execute(columns_query).fetchall()
                    wb_category_found = False
                    for col in columns:
                        print(f"  {col[0]} ({col[1]})")
                        if 'wb_category' in col[0].lower():
                            wb_category_found = True
                    
                    if wb_category_found:
                        print("  ✅ wb_category найдена!")
                        # Проверяем данные
                        count_query = f"SELECT COUNT(*) FROM {table_name}"
                        count = conn.execute(count_query).fetchone()[0]
                        print(f"  Записей: {count}")
                        
                        if count > 0:
                            wb_cat_query = f"SELECT DISTINCT wb_category FROM {table_name} WHERE wb_category IS NOT NULL LIMIT 10"
                            categories = conn.execute(wb_cat_query).fetchall()
                            print(f"  Примеры категорий:")
                            for cat in categories:
                                print(f"    {cat[0]}")
                    else:
                        print("  ❌ wb_category не найдена")
                        
                except Exception as e:
                    print(f"  Ошибка при анализе таблицы {table_name}: {e}")
        
    except Exception as e:
        print(f"Общая ошибка: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_wb_category()