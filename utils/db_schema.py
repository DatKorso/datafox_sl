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
    "oz_category_products": {
        "description": "Продукты по категориям Ozon (импорт шаблонов)",
        "file_type": "folder_xlsx",
        "read_params": {'sheet_name': "Шаблон", 'header': 1, 'skip_rows_after_header': 2},
        "config_report_key": "oz_category_products_folder",
        "columns": [
            {'target_col_name': 'oz_vendor_code', 'sql_type': 'VARCHAR', 'source_col_name': 'Артикул*', 'notes': None},
            {'target_col_name': 'product_name', 'sql_type': 'VARCHAR', 'source_col_name': 'Название товара', 'notes': None},
            {'target_col_name': 'oz_actual_price', 'sql_type': 'NUMERIC', 'source_col_name': 'Цена, руб.*', 'notes': None},
            {'target_col_name': 'oz_price_before_discount', 'sql_type': 'NUMERIC', 'source_col_name': 'Цена до скидки, руб.', 'notes': None},
            {'target_col_name': 'vat_percent', 'sql_type': 'INTEGER', 'source_col_name': 'НДС, %*', 'notes': None},
            {'target_col_name': 'installment', 'sql_type': 'VARCHAR', 'source_col_name': 'Рассрочка', 'notes': None},
            {'target_col_name': 'review_points', 'sql_type': 'INTEGER', 'source_col_name': 'Баллы за отзывы', 'notes': None},
            {'target_col_name': 'oz_sku', 'sql_type': 'VARCHAR', 'source_col_name': 'SKU', 'notes': None},
            {'target_col_name': 'barcode', 'sql_type': 'VARCHAR', 'source_col_name': 'Штрихкод (Серийный номер / EAN)', 'notes': None},
            {'target_col_name': 'package_weight_g', 'sql_type': 'INTEGER', 'source_col_name': 'Вес в упаковке, г*', 'notes': None},
            {'target_col_name': 'package_width_mm', 'sql_type': 'INTEGER', 'source_col_name': 'Ширина упаковки, мм*', 'notes': None},
            {'target_col_name': 'package_height_mm', 'sql_type': 'INTEGER', 'source_col_name': 'Высота упаковки, мм*', 'notes': None},
            {'target_col_name': 'package_length_mm', 'sql_type': 'INTEGER', 'source_col_name': 'Длина упаковки, мм*', 'notes': None},
            {'target_col_name': 'main_photo_url', 'sql_type': 'VARCHAR', 'source_col_name': 'Ссылка на главное фото*', 'notes': None},
            {'target_col_name': 'additional_photos_urls', 'sql_type': 'TEXT', 'source_col_name': 'Ссылки на дополнительные фото', 'notes': None},
            {'target_col_name': 'photo_360_urls', 'sql_type': 'TEXT', 'source_col_name': 'Ссылки на фото 360', 'notes': None},
            {'target_col_name': 'photo_article', 'sql_type': 'VARCHAR', 'source_col_name': 'Артикул фото', 'notes': None},
            {'target_col_name': 'oz_brand', 'sql_type': 'VARCHAR', 'source_col_name': 'Бренд в одежде и обуви*', 'notes': None},
            {'target_col_name': 'merge_on_card', 'sql_type': 'VARCHAR', 'source_col_name': 'Объединить на одной карточке*', 'notes': None},
            {'target_col_name': 'color', 'sql_type': 'VARCHAR', 'source_col_name': 'Цвет товара*', 'notes': None},
            {'target_col_name': 'russian_size', 'sql_type': 'VARCHAR', 'source_col_name': 'Российский размер*', 'notes': None},
            {'target_col_name': 'color_name', 'sql_type': 'VARCHAR', 'source_col_name': 'Название цвета', 'notes': None},
            {'target_col_name': 'manufacturer_size', 'sql_type': 'VARCHAR', 'source_col_name': 'Размер производителя', 'notes': None},
            {'target_col_name': 'type', 'sql_type': 'VARCHAR', 'source_col_name': 'Тип*', 'notes': None},
            {'target_col_name': 'gender', 'sql_type': 'VARCHAR', 'source_col_name': 'Пол*', 'notes': None},
            {'target_col_name': 'season', 'sql_type': 'VARCHAR', 'source_col_name': 'Сезон', 'notes': None},
            {'target_col_name': 'is_18plus', 'sql_type': 'VARCHAR', 'source_col_name': 'Признак 18+', 'notes': None},
            {'target_col_name': 'group_name', 'sql_type': 'VARCHAR', 'source_col_name': 'Название группы', 'notes': None},
            {'target_col_name': 'hashtags', 'sql_type': 'TEXT', 'source_col_name': '#Хештеги', 'notes': None},
            {'target_col_name': 'annotation', 'sql_type': 'TEXT', 'source_col_name': 'Аннотация', 'notes': None},
            {'target_col_name': 'rich_content_json', 'sql_type': 'TEXT', 'source_col_name': 'Rich-контент JSON', 'notes': None},
            {'target_col_name': 'keywords', 'sql_type': 'TEXT', 'source_col_name': 'Ключевые слова', 'notes': None},
            {'target_col_name': 'country_of_origin', 'sql_type': 'VARCHAR', 'source_col_name': 'Страна-изготовитель', 'notes': None},
            {'target_col_name': 'material', 'sql_type': 'VARCHAR', 'source_col_name': 'Материал', 'notes': None},
            {'target_col_name': 'upper_material', 'sql_type': 'VARCHAR', 'source_col_name': 'Материал верха', 'notes': None},
            {'target_col_name': 'lining_material', 'sql_type': 'VARCHAR', 'source_col_name': 'Материал подкладки обуви', 'notes': None},
            {'target_col_name': 'insole_material', 'sql_type': 'VARCHAR', 'source_col_name': 'Материал стельки', 'notes': None},
            {'target_col_name': 'outsole_material', 'sql_type': 'VARCHAR', 'source_col_name': 'Материал подошвы обуви', 'notes': None},
            {'target_col_name': 'collection', 'sql_type': 'VARCHAR', 'source_col_name': 'Коллекция', 'notes': None},
            {'target_col_name': 'style', 'sql_type': 'VARCHAR', 'source_col_name': 'Стиль', 'notes': None},
            {'target_col_name': 'temperature_mode', 'sql_type': 'VARCHAR', 'source_col_name': 'Температурный режим', 'notes': None},
            {'target_col_name': 'foot_length_cm', 'sql_type': 'NUMERIC', 'source_col_name': 'Длина стопы, см', 'notes': None},
            {'target_col_name': 'insole_length_cm', 'sql_type': 'NUMERIC', 'source_col_name': 'Длина стельки, см', 'notes': None},
            {'target_col_name': 'fullness', 'sql_type': 'VARCHAR', 'source_col_name': 'Полнота', 'notes': None},
            {'target_col_name': 'heel_height_cm', 'sql_type': 'NUMERIC', 'source_col_name': 'Высота каблука, см', 'notes': None},
            {'target_col_name': 'sole_height_cm', 'sql_type': 'NUMERIC', 'source_col_name': 'Высота подошвы, см', 'notes': None},
            {'target_col_name': 'bootleg_height_cm', 'sql_type': 'NUMERIC', 'source_col_name': 'Высота голенища, см', 'notes': None},
            {'target_col_name': 'size_info', 'sql_type': 'TEXT', 'source_col_name': 'Информация о размерах', 'notes': None},
            {'target_col_name': 'fastener_type', 'sql_type': 'VARCHAR', 'source_col_name': 'Вид застёжки', 'notes': None},
            {'target_col_name': 'heel_type', 'sql_type': 'VARCHAR', 'source_col_name': 'Вид каблука', 'notes': None},
            {'target_col_name': 'model_features', 'sql_type': 'TEXT', 'source_col_name': 'Особенности модели', 'notes': None},
            {'target_col_name': 'decorative_elements', 'sql_type': 'TEXT', 'source_col_name': 'Декоративные элементы', 'notes': None},
            {'target_col_name': 'fit', 'sql_type': 'VARCHAR', 'source_col_name': 'Посадка', 'notes': None},
            {'target_col_name': 'size_table_json', 'sql_type': 'TEXT', 'source_col_name': 'Таблица размеров JSON', 'notes': None},
            {'target_col_name': 'warranty_period', 'sql_type': 'VARCHAR', 'source_col_name': 'Гарантийный срок', 'notes': None},
            {'target_col_name': 'sport_purpose', 'sql_type': 'VARCHAR', 'source_col_name': 'Спортивное назначение', 'notes': None},
            {'target_col_name': 'orthopedic', 'sql_type': 'VARCHAR', 'source_col_name': 'Ортопедический', 'notes': None},
            {'target_col_name': 'waterproof', 'sql_type': 'VARCHAR', 'source_col_name': 'Непромокаемые', 'notes': None},
            {'target_col_name': 'brand_country', 'sql_type': 'VARCHAR', 'source_col_name': 'Страна бренда', 'notes': None},
            {'target_col_name': 'pronation_type', 'sql_type': 'VARCHAR', 'source_col_name': 'Тип пронации', 'notes': None},
            {'target_col_name': 'membrane_material_type', 'sql_type': 'VARCHAR', 'source_col_name': 'Тип мембранного материала', 'notes': None},
            {'target_col_name': 'target_audience', 'sql_type': 'VARCHAR', 'source_col_name': 'Целевая аудитория', 'notes': None},
            {'target_col_name': 'package_count', 'sql_type': 'INTEGER', 'source_col_name': 'Количество заводских упаковок', 'notes': None},
            {'target_col_name': 'tnved_codes', 'sql_type': 'VARCHAR', 'source_col_name': 'ТН ВЭД коды ЕАЭС', 'notes': None},
            {'target_col_name': 'platform_height_cm', 'sql_type': 'NUMERIC', 'source_col_name': 'Высота платформы, см', 'notes': None},
            {'target_col_name': 'boots_model', 'sql_type': 'VARCHAR', 'source_col_name': 'Модель ботинок', 'notes': None},
            {'target_col_name': 'shoes_model', 'sql_type': 'VARCHAR', 'source_col_name': 'Модель туфель', 'notes': None},
            {'target_col_name': 'ballet_flats_model', 'sql_type': 'VARCHAR', 'source_col_name': 'Модель балеток', 'notes': None},
            {'target_col_name': 'shoes_in_pack_count', 'sql_type': 'INTEGER', 'source_col_name': 'Количество пар обуви в упаковке', 'notes': None},
            {'target_col_name': 'error', 'sql_type': 'TEXT', 'source_col_name': 'Ошибка', 'notes': None},
            {'target_col_name': 'warning', 'sql_type': 'TEXT', 'source_col_name': 'Предупреждение', 'notes': None}
        ],
        "pre_update_action": "DELETE FROM oz_category_products;"
    },
    "oz_video_products": {
        "description": "Видео продукты Ozon",
        "file_type": "folder_xlsx",
        "read_params": {'sheet_name': "Озон.Видео", 'header': 1, 'skip_rows_after_header': 2},
        "config_report_key": "oz_video_products_folder",
        "columns": [
            {'target_col_name': 'oz_vendor_code', 'sql_type': 'VARCHAR', 'source_col_name': 'Артикул*', 'notes': None},
            {'target_col_name': 'video_name', 'sql_type': 'VARCHAR', 'source_col_name': 'Озон.Видео: название', 'notes': None},
            {'target_col_name': 'video_link', 'sql_type': 'TEXT', 'source_col_name': 'Озон.Видео: ссылка', 'notes': None},
            {'target_col_name': 'products_on_video', 'sql_type': 'TEXT', 'source_col_name': 'Озон.Видео: товары на видео', 'notes': None}
        ],
        "pre_update_action": "DELETE FROM oz_video_products;"
    },
    "oz_video_cover_products": {
        "description": "Видеообложки продуктов Ozon",
        "file_type": "folder_xlsx",
        "read_params": {'sheet_name': "Озон.Видеообложка", 'header': 1, 'skip_rows_after_header': 2},
        "config_report_key": "oz_video_cover_products_folder",
        "columns": [
            {'target_col_name': 'oz_vendor_code', 'sql_type': 'VARCHAR', 'source_col_name': 'Артикул*', 'notes': None},
            {'target_col_name': 'video_cover_link', 'sql_type': 'TEXT', 'source_col_name': 'Озон.Видеообложка: ссылка', 'notes': None}
        ],
        "pre_update_action": "DELETE FROM oz_video_cover_products;"
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
        "description": "Данные Punta из Google Sheets (универсальная схема)",
        "file_type": "google_sheets",
        "read_params": {},
        "config_report_key": "punta_google_sheets_url",
        "columns": "DYNAMIC",  # Указывает, что схема определяется динамически
        "pre_update_action": "DROP TABLE IF EXISTS punta_table;"  # Полная пересборка таблицы
    },
    "category_mapping": {
        "description": "Соответствие категорий между маркетплейсами",
        "file_type": "manual",  # Управляется вручную через интерфейс
        "read_params": {},
        "config_report_key": None,
        "columns": [
            {'target_col_name': 'id', 'sql_type': 'INTEGER PRIMARY KEY DEFAULT nextval(\'category_mapping_seq\')', 'source_col_name': 'id', 'notes': 'Auto-incrementing primary key using sequence'},
            {'target_col_name': 'wb_category', 'sql_type': 'VARCHAR', 'source_col_name': 'wb_category', 'notes': 'Категория Wildberries'},
            {'target_col_name': 'oz_category', 'sql_type': 'VARCHAR', 'source_col_name': 'oz_category', 'notes': 'Соответствующая категория Ozon'},
            {'target_col_name': 'created_at', 'sql_type': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP', 'source_col_name': 'created_at', 'notes': 'Дата создания записи'},
            {'target_col_name': 'notes', 'sql_type': 'TEXT', 'source_col_name': 'notes', 'notes': 'Комментарии к соответствию'}
        ],
        "pre_update_action": None  # Таблица не очищается при импорте
    }
}

def get_defined_table_names() -> list[str]:
    """Returns a list of table names defined in the hardcoded schema."""
    return list(HARDCODED_SCHEMA.keys())

def get_table_schema_definition(table_name: str) -> dict | None:
    """Returns the schema definition for a specific table from HARDCODED_SCHEMA."""
    return HARDCODED_SCHEMA.get(table_name)

def get_table_columns_from_schema(table_name: str) -> list[tuple[str, str, str, str]]:
    """
    Extracts column information from the hardcoded schema for a given table.
    
    Returns:
        List of tuples: (target_col_name, sql_type, source_col_name, notes)
        Returns empty list if table not found or uses dynamic schema.
    """
    table_schema = HARDCODED_SCHEMA.get(table_name)
    if not table_schema:
        return []
    
    columns_info = table_schema.get("columns")
    
    # Handle dynamic schema
    if columns_info == "DYNAMIC":
        return []  # Dynamic schema - no predefined columns
    
    if not columns_info:
        return []
    
    return [
        (col_def['target_col_name'], col_def['sql_type'], col_def['source_col_name'], col_def.get('notes'))
        for col_def in columns_info
    ]

def is_dynamic_schema_table(table_name: str) -> bool:
    """
    Checks if a table uses dynamic schema.
    
    Returns:
        True if table uses dynamic schema, False otherwise
    """
    table_schema = HARDCODED_SCHEMA.get(table_name)
    if not table_schema:
        return False
    
    return table_schema.get("columns") == "DYNAMIC"

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
        # First, create sequence for category_mapping if needed
        try:
            con.execute("CREATE SEQUENCE IF NOT EXISTS category_mapping_seq START 1")
        except Exception as e:
            print(f"Warning: Could not create sequence category_mapping_seq: {e}")
            if callable(st.warning): st.warning(f"Could not create sequence: {e}")
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

            # Skip dynamic schema tables - they are created during import
            if columns_data == "DYNAMIC":
                print(f"Info: Table '{table_name}' uses dynamic schema. Will be created during import.")
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