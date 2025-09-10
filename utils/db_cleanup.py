"""
Database cleanup utilities for removing duplicate, outdated, and irrelevant data.

This module provides functions to clean various types of problematic data identified
through database analysis, including duplicate barcodes, future-dated orders, 
orphaned records, and oversized text fields.
"""

import duckdb
import streamlit as st
from typing import Tuple, Dict, List
from datetime import datetime, date


def cleanup_duplicate_barcodes(db_connection: duckdb.DuckDBPyConnection) -> Tuple[bool, str, Dict]:
    """
    Remove duplicate barcodes, keeping only the most recent entry for each vendor code.
    
    Args:
        db_connection: Active DuckDB connection
        
    Returns:
        Tuple[bool, str, Dict]: Success status, message, statistics
    """
    try:
        # Get statistics before cleanup
        pre_count = db_connection.execute("SELECT COUNT(*) FROM oz_barcodes").fetchone()[0]
        duplicates_query = """
            SELECT COUNT(*) FROM (
                SELECT oz_vendor_code, COUNT(*) as cnt 
                FROM oz_barcodes 
                GROUP BY oz_vendor_code 
                HAVING COUNT(*) > 1
            )
        """
        duplicate_vendor_codes = db_connection.execute(duplicates_query).fetchone()[0]
        
        # Create temp table with deduplicated data
        # Keep the record with the largest oz_product_id (assuming it's the most recent)
        dedup_query = """
            CREATE OR REPLACE TABLE oz_barcodes_clean AS
            SELECT oz_vendor_code, oz_product_id, oz_barcode
            FROM (
                SELECT oz_vendor_code, oz_product_id, oz_barcode,
                       ROW_NUMBER() OVER (PARTITION BY oz_vendor_code ORDER BY oz_product_id DESC NULLS LAST) as rn
                FROM oz_barcodes
            ) t
            WHERE rn = 1
        """
        
        db_connection.execute(dedup_query)
        
        # Replace original table
        db_connection.execute("DROP TABLE oz_barcodes")
        db_connection.execute("ALTER TABLE oz_barcodes_clean RENAME TO oz_barcodes")

        # Recreate indexes for oz_barcodes after destructive change
        try:
            from .db_indexing import recreate_indexes_after_import
            recreate_indexes_after_import(db_connection, 'oz_barcodes', silent=True)
        except Exception:
            # Индексы не критичны для завершения очистки; проглотим, но не мешаем ходу
            pass
        
        # Get statistics after cleanup
        post_count = db_connection.execute("SELECT COUNT(*) FROM oz_barcodes").fetchone()[0]
        removed_count = pre_count - post_count
        
        stats = {
            'records_before': pre_count,
            'records_after': post_count,
            'records_removed': removed_count,
            'duplicate_vendor_codes': duplicate_vendor_codes
        }
        
        message = f"Удалено {removed_count} дублирующихся штрихкодов. Осталось {post_count} уникальных записей."
        return True, message, stats
        
    except Exception as e:
        return False, f"Ошибка при очистке дублей штрихкодов: {str(e)}", {}


def cleanup_future_dated_orders(db_connection: duckdb.DuckDBPyConnection, cutoff_date: date = None) -> Tuple[bool, str, Dict]:
    """
    Remove orders with dates in the future (likely test data).
    
    Args:
        db_connection: Active DuckDB connection
        cutoff_date: Date after which orders are considered future-dated (default: today)
        
    Returns:
        Tuple[bool, str, Dict]: Success status, message, statistics
    """
    try:
        if cutoff_date is None:
            cutoff_date = date.today()
            
        # Get statistics before cleanup
        pre_count = db_connection.execute("SELECT COUNT(*) FROM oz_orders").fetchone()[0]
        
        future_orders_query = f"""
            SELECT COUNT(*) FROM oz_orders 
            WHERE oz_accepted_date > '{cutoff_date}'
        """
        future_count = db_connection.execute(future_orders_query).fetchone()[0]
        
        # Remove future-dated orders
        cleanup_query = f"""
            DELETE FROM oz_orders 
            WHERE oz_accepted_date > '{cutoff_date}'
        """
        
        db_connection.execute(cleanup_query)
        
        # Get statistics after cleanup
        post_count = db_connection.execute("SELECT COUNT(*) FROM oz_orders").fetchone()[0]
        
        stats = {
            'records_before': pre_count,
            'records_after': post_count,
            'records_removed': future_count,
            'cutoff_date': str(cutoff_date)
        }
        
        message = f"Удалено {future_count} заказов с датами после {cutoff_date}. Осталось {post_count} записей."
        return True, message, stats
        
    except Exception as e:
        return False, f"Ошибка при очистке заказов с будущими датами: {str(e)}", {}


def cleanup_orphaned_products(db_connection: duckdb.DuckDBPyConnection) -> Tuple[bool, str, Dict]:
    """
    Remove products that don't have corresponding orders or category data.
    
    Args:
        db_connection: Active DuckDB connection
        
    Returns:
        Tuple[bool, str, Dict]: Success status, message, statistics
    """
    try:
        # Get statistics before cleanup
        pre_count = db_connection.execute("SELECT COUNT(*) FROM oz_products").fetchone()[0]
        
        # Find orphaned products (no orders and not in category products)
        orphaned_query = """
            SELECT COUNT(*) FROM oz_products ozp
            WHERE NOT EXISTS (
                SELECT 1 FROM oz_orders ozo WHERE ozo.oz_vendor_code = ozp.oz_vendor_code
            )
            AND NOT EXISTS (
                SELECT 1 FROM oz_category_products ozcp WHERE ozcp.oz_vendor_code = ozp.oz_vendor_code
            )
        """
        orphaned_count = db_connection.execute(orphaned_query).fetchone()[0]
        
        # Remove orphaned products
        cleanup_query = """
            DELETE FROM oz_products
            WHERE oz_vendor_code IN (
                SELECT ozp.oz_vendor_code FROM oz_products ozp
                WHERE NOT EXISTS (
                    SELECT 1 FROM oz_orders ozo WHERE ozo.oz_vendor_code = ozp.oz_vendor_code
                )
                AND NOT EXISTS (
                    SELECT 1 FROM oz_category_products ozcp WHERE ozcp.oz_vendor_code = ozp.oz_vendor_code
                )
            )
        """
        
        db_connection.execute(cleanup_query)
        
        # Get statistics after cleanup
        post_count = db_connection.execute("SELECT COUNT(*) FROM oz_products").fetchone()[0]
        
        stats = {
            'records_before': pre_count,
            'records_after': post_count,
            'records_removed': orphaned_count
        }
        
        message = f"Удалено {orphaned_count} товаров без заказов и категорий. Осталось {post_count} записей."
        return True, message, stats
        
    except Exception as e:
        return False, f"Ошибка при очистке товаров-сирот: {str(e)}", {}


def cleanup_empty_text_fields(db_connection: duckdb.DuckDBPyConnection, table_name: str, field_name: str, min_fill_percentage: float = 30.0) -> Tuple[bool, str, Dict]:
    """
    Clear text fields with low fill percentage to save space.
    
    Args:
        db_connection: Active DuckDB connection
        table_name: Name of the table to clean
        field_name: Name of the field to clean
        min_fill_percentage: Minimum fill percentage to keep the field
        
    Returns:
        Tuple[bool, str, Dict]: Success status, message, statistics
    """
    try:
        # Calculate current fill percentage
        stats_query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(*) FILTER (WHERE {field_name} IS NOT NULL AND {field_name} != '') as filled_records,
                SUM(LENGTH(COALESCE({field_name}, ''))) / 1024 / 1024 as size_mb
            FROM {table_name}
        """
        
        result = db_connection.execute(stats_query).fetchone()
        total_records, filled_records, size_mb = result
        
        fill_percentage = (filled_records / total_records * 100) if total_records > 0 else 0
        
        if fill_percentage < min_fill_percentage:
            # Clear the field
            cleanup_query = f"UPDATE {table_name} SET {field_name} = NULL"
            db_connection.execute(cleanup_query)
            
            stats = {
                'total_records': total_records,
                'filled_records': filled_records,
                'fill_percentage': fill_percentage,
                'size_freed_mb': size_mb,
                'field_cleared': True
            }
            
            message = f"Поле {field_name} очищено (заполненность {fill_percentage:.1f}% < {min_fill_percentage}%). Освобождено {size_mb:.1f} MB."
            return True, message, stats
        else:
            stats = {
                'total_records': total_records,
                'filled_records': filled_records,
                'fill_percentage': fill_percentage,
                'size_freed_mb': 0,
                'field_cleared': False
            }
            
            message = f"Поле {field_name} сохранено (заполненность {fill_percentage:.1f}% >= {min_fill_percentage}%)."
            return True, message, stats
            
    except Exception as e:
        return False, f"Ошибка при очистке поля {field_name}: {str(e)}", {}


def clear_table_completely(db_connection: duckdb.DuckDBPyConnection, table_name: str) -> Tuple[bool, str, Dict]:
    """
    Completely clear a table (remove all records).
    
    Args:
        db_connection: Active DuckDB connection
        table_name: Name of the table to clear
        
    Returns:
        Tuple[bool, str, Dict]: Success status, message, statistics
    """
    try:
        # Get count before deletion
        pre_count = db_connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        # Clear the table
        db_connection.execute(f"DELETE FROM {table_name}")
        
        # Verify deletion
        post_count = db_connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        stats = {
            'records_before': pre_count,
            'records_after': post_count,
            'records_removed': pre_count
        }
        
        message = f"Таблица {table_name} полностью очищена. Удалено {pre_count} записей."
        return True, message, stats
        
    except Exception as e:
        return False, f"Ошибка при очистке таблицы {table_name}: {str(e)}", {}


def vacuum_database(db_connection: duckdb.DuckDBPyConnection) -> Tuple[bool, str, Dict]:
    """
    Perform VACUUM operation to reclaim disk space after deletions.
    
    Args:
        db_connection: Active DuckDB connection
        
    Returns:
        Tuple[bool, str, Dict]: Success status, message, statistics
    """
    try:
        import os
        from utils.config_utils import get_db_path
        
        # Get DB file size before vacuum
        db_path = get_db_path()
        size_before = os.path.getsize(db_path) / (1024 * 1024)  # MB
        
        # Perform VACUUM operation
        db_connection.execute("VACUUM;")
        
        # Get DB file size after vacuum
        size_after = os.path.getsize(db_path) / (1024 * 1024)  # MB
        size_freed = size_before - size_after
        
        stats = {
            'size_before_mb': round(size_before, 2),
            'size_after_mb': round(size_after, 2),
            'size_freed_mb': round(size_freed, 2),
            'compression_percent': round((size_freed / size_before) * 100, 2) if size_before > 0 else 0
        }
        
        message = f"VACUUM завершен. Размер БД: {size_before:.1f} MB → {size_after:.1f} MB. Освобождено {size_freed:.1f} MB ({stats['compression_percent']:.1f}%)."
        return True, message, stats
        
    except Exception as e:
        return False, f"Ошибка при выполнении VACUUM: {str(e)}", {}


def create_optimized_database(db_connection: duckdb.DuckDBPyConnection) -> Tuple[bool, str, Dict]:
    """
    Create a new optimized database file by exporting and reimporting data.
    This is more effective than VACUUM for heavily fragmented databases.
    
    Args:
        db_connection: Active DuckDB connection
        
    Returns:
        Tuple[bool, str, Dict]: Success status, message, statistics
    """
    try:
        import os
        import shutil
        from datetime import datetime
        from utils.config_utils import get_db_path
        from utils.db_schema import create_tables_from_schema
        
        db_path = get_db_path()
        size_before = os.path.getsize(db_path) / (1024 * 1024)  # MB
        
        # Create backup path
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = db_path.replace('.db', f'_backup_{timestamp}.db')
        
        # Create new database path
        new_db_path = db_path.replace('.db', '_optimized.db')
        
        # Backup current database
        shutil.copy2(db_path, backup_path)
        
        # Get list of non-empty tables with their data
        tables_with_data = []
        all_tables = ['oz_orders', 'oz_barcodes', 'oz_products', 'oz_category_products', 
                     'oz_video_products', 'oz_video_cover_products', 'wb_products', 'wb_prices', 'punta_table']
        
        for table in all_tables:
            try:
                count = db_connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                if count > 0:
                    tables_with_data.append((table, count))
            except:
                pass  # Table might not exist
        
        # Create new optimized database
        new_connection = duckdb.connect(new_db_path)
        
        # Create schema in new database
        create_tables_from_schema(new_connection)
        
        # Copy data from non-empty tables
        total_records_copied = 0
        for table_name, record_count in tables_with_data:
            # Export data to temporary CSV
            temp_csv = f"temp_{table_name}.csv"
            
            # Get column names
            columns_result = db_connection.execute(f"PRAGMA table_info({table_name})").fetchall()
            columns = [col[1] for col in columns_result]
            columns_str = ', '.join([f'"{col}"' for col in columns])
            
            # Export data
            db_connection.execute(f"""
                COPY (SELECT {columns_str} FROM {table_name}) 
                TO '{temp_csv}' (FORMAT CSV, HEADER);
            """)
            
            # Import to new database
            new_connection.execute(f"""
                COPY {table_name} FROM '{temp_csv}' (FORMAT CSV, HEADER);
            """)
            
            # Cleanup temp file
            try:
                os.remove(temp_csv)
            except:
                pass
                
            total_records_copied += record_count
        
        new_connection.close()
        
        # Get new database size
        size_after = os.path.getsize(new_db_path) / (1024 * 1024)  # MB
        size_freed = size_before - size_after
        
        # Replace old database with new one
        if os.path.exists(db_path):
            os.remove(db_path)
        shutil.move(new_db_path, db_path)
        
        stats = {
            'size_before_mb': round(size_before, 2),
            'size_after_mb': round(size_after, 2),
            'size_freed_mb': round(size_freed, 2),
            'compression_percent': round((size_freed / size_before) * 100, 2) if size_before > 0 else 0,
            'backup_path': backup_path,
            'tables_copied': len(tables_with_data),
            'total_records_copied': total_records_copied
        }
        
        message = f"База данных оптимизирована! Размер: {size_before:.1f} MB → {size_after:.1f} MB. Освобождено {size_freed:.1f} MB ({stats['compression_percent']:.1f}%). Резервная копия: {backup_path}"
        return True, message, stats
        
    except Exception as e:
        return False, f"Ошибка при оптимизации БД: {str(e)}", {}


def get_cleanup_recommendations(db_connection: duckdb.DuckDBPyConnection) -> Dict:
    """
    Analyze database and provide cleanup recommendations.
    
    Args:
        db_connection: Active DuckDB connection
        
    Returns:
        Dict: Analysis results and recommendations
    """
    recommendations = []
    
    try:
        # Check for duplicate barcodes
        duplicate_barcodes = db_connection.execute("""
            SELECT COUNT(*) FROM (
                SELECT oz_vendor_code FROM oz_barcodes 
                GROUP BY oz_vendor_code HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        
        if duplicate_barcodes > 0:
            recommendations.append({
                'type': 'duplicate_barcodes',
                'severity': 'medium',
                'count': duplicate_barcodes,
                'description': f'Найдено {duplicate_barcodes} артикулов с дублирующимися штрихкодами'
            })
        
        # Check for future-dated orders
        future_orders = db_connection.execute("""
            SELECT COUNT(*) FROM oz_orders 
            WHERE oz_accepted_date > CURRENT_DATE
        """).fetchone()[0]
        
        if future_orders > 0:
            recommendations.append({
                'type': 'future_orders',
                'severity': 'high',
                'count': future_orders,
                'description': f'Найдено {future_orders} заказов с датами из будущего'
            })
        
        # Check for orphaned products
        orphaned_products = db_connection.execute("""
            SELECT COUNT(*) FROM oz_products ozp
            WHERE NOT EXISTS (
                SELECT 1 FROM oz_orders ozo WHERE ozo.oz_vendor_code = ozp.oz_vendor_code
            )
            AND NOT EXISTS (
                SELECT 1 FROM oz_category_products ozcp WHERE ozcp.oz_vendor_code = ozp.oz_vendor_code
            )
        """).fetchone()[0]
        
        if orphaned_products > 0:
            recommendations.append({
                'type': 'orphaned_products',
                'severity': 'medium',
                'count': orphaned_products,
                'description': f'Найдено {orphaned_products} товаров без заказов и категорий'
            })
        
        # Check for low-filled text fields
        low_fill_fields = db_connection.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(*) FILTER (WHERE rich_content_json IS NOT NULL AND rich_content_json != '') as rich_content_filled,
                COUNT(*) FILTER (WHERE keywords IS NOT NULL AND keywords != '') as keywords_filled
            FROM oz_category_products
        """).fetchone()
        
        total_records, rich_content_filled, keywords_filled = low_fill_fields
        
        if total_records > 0:
            rich_content_pct = (rich_content_filled / total_records) * 100
            keywords_pct = (keywords_filled / total_records) * 100
            
            if rich_content_pct < 60:
                recommendations.append({
                    'type': 'low_fill_rich_content',
                    'severity': 'low',
                    'count': rich_content_filled,
                    'percentage': rich_content_pct,
                    'description': f'Поле rich_content_json заполнено только на {rich_content_pct:.1f}%'
                })
            
            if keywords_pct < 40:
                recommendations.append({
                    'type': 'low_fill_keywords',
                    'severity': 'low',
                    'count': keywords_filled,
                    'percentage': keywords_pct,
                    'description': f'Поле keywords заполнено только на {keywords_pct:.1f}%'
                })
        
        return {
            'recommendations': recommendations,
            'total_issues': len(recommendations),
            'severity_counts': {
                'high': len([r for r in recommendations if r['severity'] == 'high']),
                'medium': len([r for r in recommendations if r['severity'] == 'medium']),
                'low': len([r for r in recommendations if r['severity'] == 'low'])
            }
        }
        
    except Exception as e:
        return {
            'error': f"Ошибка при анализе базы данных: {str(e)}",
            'recommendations': [],
            'total_issues': 0
        } 
