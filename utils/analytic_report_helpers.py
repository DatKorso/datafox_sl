"""
Helper functions for analytic report processing.

This module provides functionality to:
- Load and validate analytic report Excel files
- Map WB SKUs to Ozon SKUs by sizes
- Calculate size ranges and stock aggregations
- Generate order statistics for analytic reports
- Update Excel files with calculated data while preserving structure
"""
import pandas as pd
import openpyxl
from openpyxl import load_workbook
import os
from datetime import datetime, timedelta
import streamlit as st
from typing import Dict, List, Tuple, Optional
from utils.db_search_helpers import get_normalized_wb_barcodes, get_ozon_barcodes_and_identifiers

def load_analytic_report_file(file_path: str) -> Tuple[Optional[pd.DataFrame], Optional[openpyxl.Workbook], str]:
    """
    Loads and validates an analytic report Excel file.
    
    Args:
        file_path: Path to the Excel file
        
    Returns:
        Tuple of (DataFrame with data from row 9+, Workbook object, error message if any)
    """
    if not os.path.exists(file_path):
        return None, None, f"Файл не найден: {file_path}"
    
    try:
        # Load workbook to check structure
        wb = load_workbook(file_path)
        
        if "analytic_report" not in wb.sheetnames:
            return None, None, "Лист 'analytic_report' не найден в файле"
        
        ws = wb["analytic_report"]
        
        # Check if there are at least 9 rows
        if ws.max_row < 9:
            return None, None, "В файле недостаточно строк. Ожидается минимум 9 строк"
        
        # Load data starting from row 9 (index 8 in pandas)
        df = pd.read_excel(file_path, sheet_name="analytic_report", header=6, skiprows=[7])
        
        # Check if WB_SKU column exists
        if "WB_SKU" not in df.columns:
            return None, None, "Колонка 'WB_SKU' не найдена в 7-й строке файла"
        
        # Remove empty rows
        df = df.dropna(subset=["WB_SKU"])
        
        if df.empty:
            return None, None, "Нет данных с WB_SKU для обработки"
        
        return df, wb, ""
        
    except Exception as e:
        return None, None, f"Ошибка при загрузке файла: {str(e)}"

def map_wb_to_ozon_by_size(db_conn, wb_sku_list: List[str]) -> Dict[str, Dict[int, List[str]]]:
    """
    Maps WB SKUs to Ozon SKUs grouped by sizes.
    Uses wb_products.wb_size to determine size and maps through common barcodes.
    
    Args:
        db_conn: Database connection
        wb_sku_list: List of WB SKUs to process
        
    Returns:
        Dict mapping wb_sku -> size -> [list of oz_skus]
    """
    if not wb_sku_list:
        return {}
    
    try:
        # Step 1: Get WB products with their sizes and barcodes
        wb_skus_for_query = list(set(wb_sku_list))  # Remove duplicates
        wb_query = f"""
        SELECT 
            p.wb_sku,
            p.wb_size,
            TRIM(b.barcode) AS individual_barcode
        FROM wb_products p,
        UNNEST(regexp_split_to_array(COALESCE(p.wb_barcodes, ''), E'[\\s;]+')) AS b(barcode)
        WHERE p.wb_sku IN ({', '.join(['?'] * len(wb_skus_for_query))})
            AND NULLIF(TRIM(b.barcode), '') IS NOT NULL
        """
        
        wb_data_df = db_conn.execute(wb_query, wb_skus_for_query).fetchdf()
        
        if wb_data_df.empty:
            st.warning("Не найдены WB продукты с валидными штрихкодами")
            return {}
        
        # Step 2: Get all Ozon products and their barcodes
        oz_query = """
        SELECT DISTINCT
            p.oz_sku,
            b.oz_barcode
        FROM oz_products p
        JOIN oz_barcodes b ON p.oz_product_id = b.oz_product_id
        WHERE NULLIF(TRIM(b.oz_barcode), '') IS NOT NULL
        """
        
        oz_data_df = db_conn.execute(oz_query).fetchdf()
        
        if oz_data_df.empty:
            st.warning("Не найдены Ozon продукты с валидными штрихкодами")
            return {}
        
        # Step 3: Join WB and Ozon data by common barcodes
        wb_data_df = wb_data_df.rename(columns={'individual_barcode': 'barcode'})
        oz_data_df = oz_data_df.rename(columns={'oz_barcode': 'barcode'})
        
        # Ensure consistent data types
        wb_data_df['barcode'] = wb_data_df['barcode'].astype(str).str.strip()
        oz_data_df['barcode'] = oz_data_df['barcode'].astype(str).str.strip()
        wb_data_df['wb_sku'] = wb_data_df['wb_sku'].astype(str)
        oz_data_df['oz_sku'] = oz_data_df['oz_sku'].astype(str)
        
        # Clean empty barcodes
        wb_data_df = wb_data_df[wb_data_df['barcode'] != ''].drop_duplicates()
        oz_data_df = oz_data_df[oz_data_df['barcode'] != ''].drop_duplicates()
        
        # Join on common barcodes
        matched_df = pd.merge(wb_data_df, oz_data_df, on='barcode', how='inner')
        
        if matched_df.empty:
            st.info("Не найдено совпадений по штрихкодам между WB и Ozon")
            return {}
        
        # Step 4: Group by wb_sku and wb_size to build the result mapping
        result_map = {}
        
        for _, row in matched_df.iterrows():
            wb_sku = str(row['wb_sku'])
            wb_size = None
            
            # Try to get integer size from wb_size
            if pd.notna(row['wb_size']):
                try:
                    wb_size = int(float(row['wb_size']))
                except (ValueError, TypeError):
                    # If wb_size is not numeric, skip this record
                    continue
            
            oz_sku = str(row['oz_sku'])
            
            if wb_sku not in result_map:
                result_map[wb_sku] = {}
            
            if wb_size is not None:
                if wb_size not in result_map[wb_sku]:
                    result_map[wb_sku][wb_size] = []
                if oz_sku not in result_map[wb_sku][wb_size]:
                    result_map[wb_sku][wb_size].append(oz_sku)
        
        return result_map
        
    except Exception as e:
        st.error(f"Ошибка при сопоставлении WB и Ozon размеров: {e}")
        return {}

def calculate_size_range(size_mapping: Dict[int, List[str]]) -> str:
    """
    Calculates size range string from size mapping.
    
    Args:
        size_mapping: Dict mapping size -> [list of oz_skus]
        
    Returns:
        Size range string like "27-38" or empty string if no sizes
    """
    sizes_with_skus = [size for size, skus in size_mapping.items() if skus]
    
    if not sizes_with_skus:
        return ""
    
    min_size = min(sizes_with_skus)
    max_size = max(sizes_with_skus)
    
    if min_size == max_size:
        return str(min_size)
    else:
        return f"{min_size}-{max_size}"

def aggregate_stock_data(db_conn, oz_sku_list: List[str]) -> int:
    """
    Aggregates stock data for a list of Ozon SKUs.
    
    Args:
        db_conn: Database connection
        oz_sku_list: List of Ozon SKUs
        
    Returns:
        Total stock amount
    """
    if not oz_sku_list:
        return 0
    
    try:
        stock_query = f"""
        SELECT COALESCE(SUM(oz_fbo_stock), 0) as total_stock
        FROM oz_products 
        WHERE oz_sku IN ({', '.join(['?'] * len(oz_sku_list))})
        """
        result = db_conn.execute(stock_query, oz_sku_list).fetchdf()
        return int(result.iloc[0]['total_stock']) if not result.empty else 0
    except Exception as e:
        st.error(f"Ошибка при получении данных о остатках: {e}")
        return 0

def generate_order_statistics(db_conn, oz_sku_list: List[str], days_back: int = 30) -> Dict[str, int]:
    """
    Generates order statistics for Ozon SKUs for the last N days.
    
    Args:
        db_conn: Database connection
        oz_sku_list: List of Ozon SKUs
        days_back: Number of days to look back
        
    Returns:
        Dict mapping date_string (YYYY-MM-DD) -> order_count
    """
    if not oz_sku_list:
        return {}
    
    try:
        today = datetime.now()
        start_date = (today - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        orders_query = f"""
        SELECT 
            oz_accepted_date,
            COUNT(*) as order_count
        FROM oz_orders
        WHERE oz_sku IN ({', '.join(['?'] * len(oz_sku_list))})
            AND oz_accepted_date >= ?
            AND order_status != 'Отменён'
        GROUP BY oz_accepted_date
        ORDER BY oz_accepted_date DESC
        """
        
        orders_df = db_conn.execute(orders_query, oz_sku_list + [start_date]).fetchdf()
        
        if orders_df.empty:
            return {}
        
        # Convert to dict
        orders_df['oz_accepted_date'] = pd.to_datetime(orders_df['oz_accepted_date']).dt.strftime('%Y-%m-%d')
        return dict(zip(orders_df['oz_accepted_date'], orders_df['order_count']))
        
    except Exception as e:
        st.error(f"Ошибка при получении статистики заказов: {e}")
        return {}

def create_backup_file(file_path: str) -> str:
    """
    Creates a backup of the original file with timestamp.
    
    Args:
        file_path: Path to the original file
        
    Returns:
        Path to the backup file
    """
    directory = os.path.dirname(file_path)
    filename = os.path.basename(file_path)
    name, ext = os.path.splitext(filename)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"{name}_backup_{timestamp}{ext}"
    backup_path = os.path.join(directory, backup_filename)
    
    # Copy the file
    import shutil
    shutil.copy2(file_path, backup_path)
    
    return backup_path

def update_analytic_report(file_path: str, wb_sku_data: Dict[str, Dict]) -> Tuple[bool, str]:
    """
    Updates the analytic report Excel file with calculated data.
    
    Args:
        file_path: Path to the Excel file
        wb_sku_data: Dict containing calculated data for each WB SKU
        
    Returns:
        Tuple of (success, error_message)
    """
    try:
        # Create backup only for non-temporary files
        backup_created = False
        if not os.path.basename(file_path).startswith('temp_'):
            backup_path = create_backup_file(file_path)
            st.info(f"Создана резервная копия: {os.path.basename(backup_path)}")
            backup_created = True
        
        # Load workbook
        wb = load_workbook(file_path)
        ws = wb["analytic_report"]
        
        # Find column indices from row 7 (header row)
        header_row = 7
        column_map = {}
        
        for col_num in range(1, ws.max_column + 1):
            cell_value = ws.cell(row=header_row, column=col_num).value
            if cell_value:
                column_map[str(cell_value).strip()] = col_num
        
        # Process data starting from row 9
        data_start_row = 9
        
        for row_num in range(data_start_row, ws.max_row + 1):
            wb_sku_cell = ws.cell(row=row_num, column=column_map.get("WB_SKU", 2))
            wb_sku = str(wb_sku_cell.value).strip() if wb_sku_cell.value else ""
            
            if not wb_sku or wb_sku not in wb_sku_data:
                continue
            
            data = wb_sku_data[wb_sku]
            
            # Update size columns (OZ_SIZE_22 to OZ_SIZE_44)
            for size in range(22, 45):
                col_name = f"OZ_SIZE_{size}"
                if col_name in column_map:
                    col_num = column_map[col_name]
                    oz_skus = data.get('sizes', {}).get(size, [])
                    ws.cell(row=row_num, column=col_num).value = ';'.join(oz_skus) if oz_skus else ""
            
            # Update OZ_SIZES
            if "OZ_SIZES" in column_map:
                ws.cell(row=row_num, column=column_map["OZ_SIZES"]).value = data.get('size_range', '')
            
            # Update OZ_STOCK
            if "OZ_STOCK" in column_map:
                ws.cell(row=row_num, column=column_map["OZ_STOCK"]).value = data.get('total_stock', 0)
            
            # Update order statistics columns (ORDERS_TODAY-30 to ORDERS_TODAY-1)
            orders_data = data.get('orders', {})
            today = datetime.now()
            for days_back in range(1, 31):
                col_name = f"ORDERS_TODAY-{days_back}"
                if col_name in column_map:
                    target_date = (today - timedelta(days=days_back)).strftime('%Y-%m-%d')
                    order_count = orders_data.get(target_date, 0)
                    ws.cell(row=row_num, column=column_map[col_name]).value = order_count
        
        # Update all PUNTA_ columns dynamically
        update_punta_columns_dynamically(ws, column_map, wb_sku_data, data_start_row)
        
        # Save the updated file
        wb.save(file_path)
        return True, ""
        
    except Exception as e:
        return False, f"Ошибка при обновлении файла: {str(e)}"

def process_analytic_report(db_conn, file_path: str) -> Tuple[bool, str, Dict]:
    """
    Main function to process the entire analytic report.
    
    Args:
        db_conn: Database connection
        file_path: Path to the Excel file
        
    Returns:
        Tuple of (success, error_message, statistics_dict)
    """
    # Load and validate file
    df, wb, error_msg = load_analytic_report_file(file_path)
    if df is None:
        return False, error_msg, {}
    
    wb_sku_list = [str(sku) for sku in df['WB_SKU'].dropna().unique()]
    
    if not wb_sku_list:
        return False, "Нет валидных WB SKU для обработки", {}
    
    # Process each WB SKU
    wb_sku_data = {}
    
    # Get size mappings for all WB SKUs at once
    size_mappings = map_wb_to_ozon_by_size(db_conn, wb_sku_list)
    
    # Get Punta data for all WB SKUs at once
    punta_mappings = get_punta_data(db_conn, wb_sku_list)
    
    for wb_sku in wb_sku_list:
        size_mapping = size_mappings.get(wb_sku, {})
        
        # Get all oz_skus for this wb_sku
        all_oz_skus = []
        for oz_skus_list in size_mapping.values():
            all_oz_skus.extend(oz_skus_list)
        
        # Calculate size range
        size_range = calculate_size_range(size_mapping)
        
        # Get stock data
        total_stock = aggregate_stock_data(db_conn, all_oz_skus)
        
        # Get order statistics
        orders_data = generate_order_statistics(db_conn, all_oz_skus, days_back=30)
        
        # Get Punta data
        punta_data = punta_mappings.get(wb_sku, {})
        
        wb_sku_data[wb_sku] = {
            'sizes': size_mapping,
            'size_range': size_range,
            'total_stock': total_stock,
            'orders': orders_data,
            'punta': punta_data
        }
    
    # Update the file
    success, error_msg = update_analytic_report(file_path, wb_sku_data)
    
    if success:
        stats = {
            'processed_wb_skus': len(wb_sku_list),
            'wb_skus_with_ozon_matches': len([wb for wb, data in wb_sku_data.items() if data['sizes']]),
            'total_ozon_skus_found': sum(len([sku for skus_list in data['sizes'].values() for sku in skus_list]) for data in wb_sku_data.values()),
            'total_stock': sum(data['total_stock'] for data in wb_sku_data.values()),
            'wb_skus_with_punta_data': len([wb for wb, data in wb_sku_data.items() if any(data['punta'].values())])
        }
        return True, "", stats
    else:
        return False, error_msg, {}

def get_punta_data(db_conn, wb_sku_list: List[str]) -> Dict[str, Dict[str, str]]:
    """
    Retrieves Punta data for given WB SKUs.
    Now works with dynamic punta_table schema - automatically detects available columns.
    
    Args:
        db_conn: Database connection
        wb_sku_list: List of WB SKUs to fetch data for
        
    Returns:
        Dict mapping wb_sku -> {column_name: value}
    """
    if not wb_sku_list:
        return {}
    
    try:
        # Import the new function to get dynamic columns
        from .db_crud import get_punta_table_columns
        
        # Get all available columns dynamically
        available_columns = get_punta_table_columns(db_conn)
        
        if not available_columns:
            st.info("📋 Таблица punta_table не существует или пуста")
            return {}
        
        # Remove wb_sku from list of data columns (it's the key column)
        data_columns = [col for col in available_columns if col != 'wb_sku']
        
        if not data_columns:
            st.warning("⚠️ В таблице punta_table нет колонок данных (только wb_sku)")
            return {}
        
        st.info(f"🔍 Найдено {len(data_columns)} колонок в punta_table: {', '.join(data_columns)}")
        
        # Convert wb_sku_list to integers for proper matching with punta_table
        wb_skus_int = []
        for wb_sku in wb_sku_list:
            try:
                wb_skus_int.append(int(wb_sku))
            except (ValueError, TypeError):
                # Skip invalid wb_sku values
                st.warning(f"Невалидный WB SKU для поиска в Punta: {wb_sku}")
                continue
        
        if not wb_skus_int:
            return {}
        
        # Build query to get punta data with dynamic columns
        if 'wb_sku' in available_columns:
            select_columns = ['wb_sku'] + data_columns
            
            punta_query = f"""
            SELECT {', '.join(f'"{col}"' for col in select_columns)}
            FROM punta_table 
            WHERE wb_sku IN ({', '.join(['?'] * len(wb_skus_int))})
            """
            
            punta_df = db_conn.execute(punta_query, wb_skus_int).fetchdf()
            
            if punta_df.empty:
                st.info(f"📭 Данные Punta не найдены для {len(wb_skus_int)} WB SKU")
                return {}
            
            st.success(f"✅ Найдены данные Punta для {len(punta_df)} из {len(wb_skus_int)} WB SKU")
            
            # Convert to dict format
            result_map = {}
            for _, row in punta_df.iterrows():
                wb_sku = str(row['wb_sku'])  # Convert back to string for consistency with input
                result_map[wb_sku] = {}
                
                for column in data_columns:
                    if column in row and pd.notna(row[column]):
                        result_map[wb_sku][column] = str(row[column])
                    else:
                        result_map[wb_sku][column] = ""
            
            return result_map
        else:
            st.error("❌ Колонка wb_sku не найдена в таблице punta_table")
            return {}
        
    except Exception as e:
        st.warning(f"Ошибка при получении данных Punta: {e}")
        return {}

def get_dynamic_punta_columns(db_conn) -> List[str]:
    """
    Получает список доступных колонок из punta_table для создания PUNTA_ колонок.
    
    Returns:
        List of column names available in punta_table (excluding wb_sku)
    """
    try:
        from .db_crud import get_punta_table_columns
        
        all_columns = get_punta_table_columns(db_conn)
        
        # Remove wb_sku as it's the key column, not data column
        data_columns = [col for col in all_columns if col != 'wb_sku']
        
        return data_columns
        
    except Exception as e:
        st.warning(f"Не удалось получить динамические колонки Punta: {e}")
        return []

def update_punta_columns_dynamically(ws, column_map: Dict[str, int], wb_sku_data: Dict[str, Dict], 
                                   data_start_row: int = 9):
    """
    Динамически обновляет все PUNTA_ колонки в Excel файле.
    Автоматически обрабатывает любые колонки, которые начинаются с PUNTA_.
    
    Args:
        ws: Excel worksheet object
        column_map: Mapping of column names to column numbers
        wb_sku_data: Data for each WB SKU
        data_start_row: Row number where data starts (default 9)
    """
    # Find all PUNTA_ columns in the Excel file
    punta_columns = {col_name: col_num for col_name, col_num in column_map.items() 
                     if col_name.startswith('PUNTA_')}
    
    if not punta_columns:
        st.info("📋 PUNTA_ колонки не найдены в аналитическом отчете")
        return
    
    st.info(f"🔄 Обновление {len(punta_columns)} PUNTA_ колонок: {', '.join(punta_columns.keys())}")
    
    # Process each row
    for row_num in range(data_start_row, ws.max_row + 1):
        wb_sku_cell = ws.cell(row=row_num, column=column_map.get("WB_SKU", 2))
        wb_sku = str(wb_sku_cell.value).strip() if wb_sku_cell.value else ""
        
        if not wb_sku or wb_sku not in wb_sku_data:
            continue
        
        punta_data = wb_sku_data[wb_sku].get('punta', {})
        
        # Update each PUNTA_ column
        for col_name, col_num in punta_columns.items():
            # Extract the actual column name (remove PUNTA_ prefix)
            punta_col_name = col_name[6:]  # Remove 'PUNTA_' prefix
            punta_value = punta_data.get(punta_col_name, '')
            ws.cell(row=row_num, column=col_num).value = punta_value 