import duckdb
import streamlit as st # For messages during schema creation
import pandas as pd # For pd.NA

# Hardcoded schema definition
# This structure will replace the parsing of mp_reports_schema.md for application logic
# mp_reports_schema.md will remain for documentation purposes only.
HARDCODED_SCHEMA = {
    "oz_orders": {
        "description": "Заказы Ozon",
        "file_type": "csv", # For import page
        "read_params": {'delimiter': ';', 'header': 0}, # For import page
        "config_report_key": "oz_orders_csv",
        "columns": [
            {'target_col_name': 'oz_order_number',    'sql_type': 'VARCHAR', 'source_col_name': 'Номер заказа', 'notes': None},
            {'target_col_name': 'oz_shipment_number', 'sql_type': 'VARCHAR', 'source_col_name': 'Номер отправления', 'notes': None},
            {'target_col_name': 'oz_accepted_date',   'sql_type': 'DATE',    'source_col_name': 'Принят в обработку', 'notes': "convert to date"},
            {'target_col_name': 'order_status',       'sql_type': 'VARCHAR', 'source_col_name': 'Статус', 'notes': None}, # Renamed from oz_status for consistency
            {'target_col_name': 'oz_sku',             'sql_type': 'BIGINT',  'source_col_name': 'OZON id', 'notes': None},
            {'target_col_name': 'oz_vendor_code',     'sql_type': 'VARCHAR', 'source_col_name': 'Артикул', 'notes': None}
        ],
        "pre_update_action": "DELETE FROM oz_orders;" # SQL to execute before import
    },
    "oz_products": {
        "description": "Товары Ozon",
        "file_type": "csv",
        "read_params": {'delimiter': ';', 'header': 0},
        "config_report_key": "oz_products_csv",
        "columns": [
            {'target_col_name': 'oz_vendor_code',     'sql_type': 'VARCHAR', 'source_col_name': 'Артикул', 'notes': "remove_single_quotes"},
            {'target_col_name': 'oz_product_id',      'sql_type': 'BIGINT',  'source_col_name': 'Ozon Product ID', 'notes': None},
            {'target_col_name': 'oz_sku',             'sql_type': 'BIGINT',  'source_col_name': 'SKU', 'notes': None},
            {'target_col_name': 'oz_brand',           'sql_type': 'VARCHAR', 'source_col_name': 'Бренд', 'notes': None},
            {'target_col_name': 'oz_product_status',  'sql_type': 'VARCHAR', 'source_col_name': 'Статус товара', 'notes': None},
            {'target_col_name': 'oz_product_visible', 'sql_type': 'VARCHAR', 'source_col_name': 'Видимость на Ozon', 'notes': None},
            {'target_col_name': 'oz_hiding_reasons',  'sql_type': 'VARCHAR', 'source_col_name': 'Причины скрытия', 'notes': None},
            {'target_col_name': 'oz_fbo_stock',       'sql_type': 'INTEGER', 'source_col_name': 'Доступно к продаже по схеме FBO, шт.', 'notes': None},
            {'target_col_name': 'oz_actual_price',    'sql_type': 'DOUBLE',  'source_col_name': 'Текущая цена с учетом скидки, ₽', 'notes': "round_to_integer"}
        ],
        "pre_update_action": "DELETE FROM oz_products;"
    },
    "oz_barcodes": {
        "description": "Штрихкоды Ozon",
        "file_type": "xlsx",
        "read_params": {'sheet_name': "Штрихкоды", 'header': 2, 'skip_rows_after_header': 1},
        "config_report_key": "oz_barcodes_xlsx",
        "columns": [
            {'target_col_name': 'oz_vendor_code', 'sql_type': 'VARCHAR', 'source_col_name': 'Артикул', 'notes': None},
            {'target_col_name': 'oz_product_id',  'sql_type': 'BIGINT',  'source_col_name': 'Ozon Product ID', 'notes': None},
            {'target_col_name': 'oz_barcode',     'sql_type': 'VARCHAR', 'source_col_name': 'Штрихкод', 'notes': None}
        ],
        "pre_update_action": "DELETE FROM oz_barcodes;"
    },
    "wb_products": {
        "description": "Товары Wildberries",
        "file_type": "folder_xlsx", # Special type for import page: folder of xlsx
        "read_params": {'sheet_name': "Товары", 'header': 2, 'skip_rows_after_header': 1},
        "config_report_key": "wb_products_dir",
        "columns": [
            {'target_col_name': 'wb_sku',      'sql_type': 'INTEGER', 'source_col_name': 'Артикул WB', 'notes': None},
            {'target_col_name': 'wb_category', 'sql_type': 'VARCHAR', 'source_col_name': 'Категория продавца', 'notes': None},
            {'target_col_name': 'wb_brand',    'sql_type': 'VARCHAR', 'source_col_name': 'Бренд', 'notes': None},
            {'target_col_name': 'wb_barcodes', 'sql_type': 'VARCHAR', 'source_col_name': 'Баркод', 'notes': None}, # Contains list, normalized by _get_normalized_wb_barcodes
            {'target_col_name': 'wb_size',     'sql_type': 'INTEGER', 'source_col_name': 'Размер', 'notes': None}
        ],
        "pre_update_action": "DELETE FROM wb_products;"
    },
    "wb_prices": {
        "description": "Цены Wildberries",
        "file_type": "xlsx",
        "read_params": {'sheet_name': "Отчет - цены и скидки на товары", 'header': 0},
        "config_report_key": "wb_prices_xlsx",
        "columns": [
            {'target_col_name': 'wb_sku',        'sql_type': 'INTEGER', 'source_col_name': 'Артикул WB', 'notes': None},
            {'target_col_name': 'wb_fbo_stock',  'sql_type': 'INTEGER', 'source_col_name': 'Остатки WB', 'notes': None},
            {'target_col_name': 'wb_full_price', 'sql_type': 'INTEGER', 'source_col_name': 'Текущая цена', 'notes': None},
            {'target_col_name': 'wb_discount',   'sql_type': 'INTEGER', 'source_col_name': 'Текущая скидка', 'notes': None}
        ],
        "pre_update_action": "DELETE FROM wb_prices;"
    },
    "punta_table": {
        "description": "Данные Punta из Google Sheets",
        "file_type": "google_sheets",
        "read_params": {},
        "config_report_key": "punta_google_sheets_url",
        "columns": [
            {'target_col_name': 'wb_sku',      'sql_type': 'INTEGER', 'source_col_name': 'wb_sku', 'notes': 'convert_to_integer'},
            {'target_col_name': 'gender',     'sql_type': 'VARCHAR', 'source_col_name': 'gender', 'notes': None},
            {'target_col_name': 'season',     'sql_type': 'VARCHAR', 'source_col_name': 'season', 'notes': None},
            {'target_col_name': 'model_name', 'sql_type': 'VARCHAR', 'source_col_name': 'model_name', 'notes': None},
            {'target_col_name': 'material',   'sql_type': 'VARCHAR', 'source_col_name': 'material', 'notes': None},
            {'target_col_name': 'new_last',   'sql_type': 'VARCHAR', 'source_col_name': 'new_last', 'notes': None},
            {'target_col_name': 'mega_last',  'sql_type': 'VARCHAR', 'source_col_name': 'mega_last', 'notes': None},
            {'target_col_name': 'best_last',  'sql_type': 'VARCHAR', 'source_col_name': 'best_last', 'notes': None}
        ],
        "pre_update_action": "DELETE FROM punta_table;"
    }
}

def get_defined_table_names() -> list[str]:
    """Returns a list of table names defined in the hardcoded schema."""
    return list(HARDCODED_SCHEMA.keys())

def get_table_schema_definition(table_name: str) -> dict | None:
    """Returns the schema definition for a specific table from HARDCODED_SCHEMA."""
    return HARDCODED_SCHEMA.get(table_name)

def get_table_columns_from_schema(table_name: str) -> list:
    """
    Helper function to get column details for a specific table from the hardcoded schema.
    Returns list of (target_col_name, sql_type, source_col_name, notes).
    If table_name is not found, returns an empty list.
    """
    table_def = HARDCODED_SCHEMA.get(table_name)
    if table_def and "columns" in table_def:
        return [
            (
                col.get('target_col_name'),
                col.get('sql_type'),
                col.get('source_col_name'),
                col.get('notes')
            ) for col in table_def["columns"]
        ]
    return []

def create_tables_from_schema(con: duckdb.DuckDBPyConnection) -> bool:
    """
    Creates tables in the database based on the hardcoded schema (HARDCODED_SCHEMA),
    if they don't already exist.
    Renamed from create_schema_if_not_exists for clarity.
    """
    if not con:
        # Use print if Streamlit context (st.*) is not guaranteed or suitable here
        print("Error: Database connection not available for schema creation.")
        # Consider if st.error is appropriate or if this module should be UI-agnostic.
        # For now, keeping print as it's a backend utility.
        return False

    defined_tables = HARDCODED_SCHEMA
    
    if not defined_tables:
        print("Critical Error: Hardcoded schema (HARDCODED_SCHEMA) is empty.")
        return False

    all_successful = True
    try:
        for table_name, table_definition in defined_tables.items():
            res = con.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table_name}';").fetchone()
            if res:
                # Optional: print(f"Info: Table '{table_name}' already exists. Skipping creation.")
                continue
            
            columns_data = table_definition.get("columns")
            if not columns_data:
                msg = f"Warning: No columns defined for table '{table_name}' in HARDCODED_SCHEMA. Skipping creation."
                print(msg)
                if callable(st.warning): st.warning(msg) # Show in UI if possible
                continue

            cols_definitions = []
            for col_def in columns_data:
                col_name = col_def.get('target_col_name')
                col_type = col_def.get('sql_type')
                if col_name and col_type:
                    cols_definitions.append(f'"{col_name}" {col_type}')
                else:
                    msg = f"Warning: Invalid column definition for table '{table_name}': {col_def}. Skipping column."
                    print(msg)
                    if callable(st.warning): st.warning(msg)

            if not cols_definitions:
                msg = f"Error: No valid column definitions found for table '{table_name}'. Skipping table creation."
                print(msg)
                if callable(st.error): st.error(msg)
                all_successful = False
                continue
            
            create_table_sql = f"CREATE TABLE \"{table_name}\" ({', '.join(cols_definitions)});"
            
            try:
                con.execute(create_table_sql)
                success_msg = f"Table '{table_name}' created successfully."
                print(success_msg)
                if callable(st.success): st.success(success_msg)
            except Exception as e:
                all_successful = False
                error_msg = f"Failed to create table '{table_name}'. SQL: {create_table_sql}. Error: {e}"
                print(error_msg)
                if callable(st.error): st.error(error_msg)
        
        return all_successful
    except Exception as e:
        error_msg = f"Error: An unexpected error occurred during schema creation: {e}"
        print(error_msg)
        if callable(st.error): st.error(error_msg)
        return False 