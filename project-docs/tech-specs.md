# Technical Specifications

## Technology Stack

*   **Programming Language:** Python 3.x
*   **Web Framework:** Streamlit
*   **Database:** DuckDB
*   **Core Data Manipulation:** Pandas
*   **File Handling:** Openpyxl (for `.xlsx`)

## Development Methods

*   **Environment:** Virtual environment (e.g., `venv`)
*   **Version Control:** Git (recommended)
*   **Modularity:** Code organized into `pages/` for UI, `utils/` for helpers (db connection, CRUD, search, analytics, theme styling).

## Coding Standards

*   PEP 8 for Python code styling.
*   Clear and concise comments for functions, classes, and complex logic blocks.
*   Type hinting where appropriate.
*   Error handling (try-except blocks) for database operations, file I/O, etc.

## Utility Modules

### Core Utilities
*   **`utils/db_connection.py`:** Database connection management
    *   `connect_db()`: Establishes connection to DuckDB database
    *   `test_db_connection()`: Tests database connectivity
    *   `get_connection_and_ensure_schema()`: Cached function for getting connection with schema validation
*   **`utils/db_crud.py`:** Database CRUD operations
*   **`utils/db_search_helpers.py`:** Cross-marketplace search functionality
*   **`utils/config_utils.py`:** Configuration file management

### Data Processing
*   **`utils/data_cleaner.py`:** Data cleaning and validation utilities
    *   `clean_integer_field()`: Cleans and converts values to integers with error handling
    *   `clean_double_field()`: Cleans and converts values to floats with error handling
    *   `apply_data_cleaning()`: Applies comprehensive data cleaning based on schema
    *   `display_cleaning_report()`: Shows detailed cleaning results in Streamlit UI
    *   `validate_required_fields()`: Validates presence and completeness of required fields

### UI and Styling
*   **`utils/theme_utils.py`:** Theme and styling utilities for Streamlit interface
    *   `apply_theme()`: Applies custom CSS styling to improve UI appearance
    *   `apply_dark_theme()`: Alternative dark theme for user preference
    *   `get_custom_colors()`: Provides color palette for charts and visualizations
    *   `style_metric_cards()`: Custom styling for metric display cards

## Database Design

*   **Primary Database:** DuckDB (file-based).
*   **Schema Management:** Defined structures for marketplace reports (e.g., Ozon orders, WB products). Schemas might be documented in `project-docs/mp_reports_schema.md` or within Python constants/dictionaries.
*   **Key Tables (examples, actual tables depend on imported reports):
    *   `oz_products`: Ozon product information.
    *   `oz_barcodes`: Ozon product barcodes (potentially linking multiple barcodes to one product ID).
    *   `oz_prices`: Ozon product prices.
    *   `oz_orders`: Ozon order details (with `oz_sku`, `oz_accepted_date`, `order_status`).
    *   `wb_products`: Wildberries product information (including `wb_barcodes` as a string, which requires normalization for barcode matching).
    *   `wb_prices`: Wildberries product prices.
    *   `wb_stock`: Wildberries stock information.
    *   `punta_table`: Product data from Punta (imported from Google Sheets).
*   **Relationships:** Primarily through common identifiers like SKUs or derived common barcodes for cross-marketplace analysis.

## Data Flow
1.  **Configuration:** User sets DB path and report paths in Settings (`config.json`).
2.  **Data Import:** User uploads files or uses configured paths. Data is read, cleaned, transformed, and loaded into DuckDB tables.
3.  **Data Access:** Streamlit pages query the DuckDB using helper functions.
    *   `utils/db_crud.py` for general data retrieval and statistics.
    *   `utils/db_search_helpers.py` for cross-marketplace linking (used by Cross-Marketplace Search and Ozon Order Statistics by WB SKU).
    *   `pages/6_Ozon_Order_Stats.py` contains logic for querying `oz_orders` and processing for statistics display.
4.  **Display:** Pandas DataFrames are used to structure data for display in Streamlit (tables, charts).

## Future Considerations / Potential Improvements

*   **Schema Migration:** A more robust system for schema migrations if table structures change significantly over time.
*   **Advanced Data Validation:** More comprehensive data validation rules during import.
*   **Logging:** Implement more detailed logging for import processes and errors.
*   **Asynchronous Operations:** For very large file imports, consider asynchronous operations to avoid blocking the UI (Streamlit has limited support here, might require architectural changes).
*   **Testing:** Introduce unit and integration tests.

## Project File Structure
- (To be detailed further in `user-structure.md` and updated post-refactoring)

## Configuration File (`config.json` example)
```json
{
  "database_path": "data/marketplace_data.db",
  "report_paths": {
    "oz_barcodes_xlsx": "data/ozon/oz_barcodes.xlsx",
    "oz_orders_csv": "data/ozon/oz_orders.csv",
    "oz_prices_xlsx": "data/ozon/oz_prices.xlsx",
    "oz_products_csv": "data/ozon/oz_products.csv",
    "wb_prices_xlsx": "data/wb/wb_prices.xlsx",
    "wb_products_dir": "data/wb/products/"
  }
}
```

### Data Ingestion and Processing

-   **Libraries**: Pandas for data manipulation, DuckDB for SQL database operations.
-   **File Formats**: Initial support for CSV and Excel (`.xlsx`).
-   **Excel Reading Engine**: `openpyxl` is the preferred engine for reading `.xlsx` files due to its robustness with complex formatting like merged cells. Pandas `read_excel` is configured to use `openpyxl`.
-   **Schema Management**: Database schema (table names, columns, types, transformations) is hardcoded in `utils/db_utils.py` in the `HARDCODED_SCHEMA` dictionary. The `mp_reports_schema.md` file serves as human-readable documentation of this schema.
-   **Data Import**:
    -   Uses `pd.read_csv()` for CSV files and `pd.read_excel(engine='openpyxl')` for Excel files.
    -   Handles column renaming based on `source_column_name` from `HARDCODED_SCHEMA`.
    -   Applies pre-update actions (e.g., deleting old data for a specific period/report) and data type conversions.
    -   Specific data cleaning and transformation (e.g., date parsing, string manipulation, numeric conversions) are applied during the import process as defined in `pre_update_actions` and inferred from target data types in `HARDCODED_SCHEMA`.
    -   For Excel files with merged cells or complex layouts (e.g., `oz_barcodes.xlsx`, `wb_products.xlsx`), if direct reading with `pd.read_excel` fails even with `engine='openpyxl'`, a fallback to direct parsing using the `openpyxl` library might be necessary to precisely extract headers and data rows before constructing a DataFrame.
-   **Error Handling**: Basic error handling for file operations and database interactions. Logging to be enhanced.

## Database Schema Changes

### Migration from INTEGER to BIGINT (v1.1)
**Date**: Current
**Reason**: Ozon SKU and Product ID values now exceed INT32 maximum value (2,147,483,647)

**Affected Tables and Columns:**
- `oz_orders.oz_sku`: INTEGER → BIGINT
- `oz_products.oz_product_id`: INTEGER → BIGINT  
- `oz_products.oz_sku`: INTEGER → BIGINT
- `oz_barcodes.oz_product_id`: INTEGER → BIGINT

**Migration Process:**
- Automatic detection via `utils/db_migration.py`
- Tables are dropped and recreated with new schema
- Data must be re-imported after migration
- Migration UI available on Home page

### Ozon Barcode Report Column Changes
**File**: oz_barcodes.xlsx
**Changes**:
- `"Артикул товара"` → `"Артикул"`
- `"Ozon ID"` → `"Ozon Product ID"`

## Utility Modules

### Core Database Management
- **`utils/db_connection.py`** - Database connection management
- **`utils/db_schema.py`** - Table schema definitions and creation
- **`utils/db_crud.py`** - Database CRUD operations and statistics
- **`utils/db_migration.py`** - Schema migration utilities  
- **`utils/data_cleaner.py`** - Data validation and cleaning functions
- **`utils/db_search_helpers.py`** - Cross-marketplace search functions

### Configuration and UI
- **`utils/config_utils.py`** - Configuration file management
- **`utils/theme_utils.py`** - UI theming and styling functions

### Data Processing Notes
- All SKU fields now support BIGINT values (up to 9,223,372,036,854,775,807)
- Data cleaning handles both INTEGER and BIGINT type validation
- String-to-number conversion preserves large values without overflow
- Cross-marketplace searches maintain string-based comparisons for reliability 

- **BIGINT Support**: All ID columns use BIGINT to handle large integers from marketplaces
- **Data Cleaning**: Intelligent text-to-number conversion with error handling
- **Migration Support**: Automatic schema updates and data type migrations

### External Integrations
- **Google Sheets API**: Direct import from Google Sheets documents via CSV export
  - Supports public and shared documents
  - Automatic URL conversion and validation
  - Real-time data synchronization
  - **UTF-8 Encoding Support**: Multi-method approach for handling Cyrillic and special characters
    - Primary method: Force UTF-8 encoding on HTTP response
    - Fallback 1: Direct UTF-8 decoding of response content bytes
    - Fallback 2: UTF-8 BOM handling for Excel-exported sheets
    - Fallback 3: Pandas UTF-8 encoding parameter for BytesIO streams
  - **Encoding Diagnostics**: Built-in tools for detecting and troubleshooting character encoding issues
    - Automatic detection of encoding type
    - Cyrillic character presence validation
    - Content sampling for visual verification
    - Actionable recommendations for encoding problems

## Database Schema

### Hardcoded Schema Definition
All table schemas are defined in `utils/db_schema.py` using the `HARDCODED_SCHEMA` dictionary. This provides:
- **Table Structure**: Column names, SQL types, source column mappings
- **Import Configuration**: File types, read parameters, config keys  
- **Data Transformation Rules**: Notes for type conversion, cleaning, validation
- **Pre-Update Actions**: SQL to execute before data import (usually DELETE FROM table)

### Schema Evolution
- **wb_sku Standardization**: All tables use INTEGER type for wb_sku to ensure proper joins
- **Type Conversion**: String wb_sku values are automatically converted during import using `convert_to_integer` notes
- **Migration Scripts**: Available in `utils/migrate_*.py` for schema changes

### Data Type Handling
- **Automatic Cleaning**: Invalid values are converted to NULL with logging
- **Type Safety**: Mismatched types between tables are prevented through schema enforcement
- **Conversion Tracking**: Import process logs all type conversion issues and statistics