# BIGINT Migration Fix for SKU Fields

## Problem Description

When importing "Штрихкоды Ozon" (Ozon Barcodes) data, users encountered the error:
```
Ошибка импорта в таблицу 'oz_barcodes': Error importing data into table 'oz_barcodes': Invalid Input Error: Python Conversion Failure: Value out of range for type INT
```

## Root Cause

The issue was caused by SKU and Product ID values in marketplace reports exceeding the maximum value for 32-bit integers (2,147,483,647). Modern marketplace systems use larger identifier values that require 64-bit integers (BIGINT).

## Affected Tables and Fields

### Ozon Tables
- `oz_orders.oz_sku`: INTEGER → BIGINT
- `oz_products.oz_product_id`: INTEGER → BIGINT  
- `oz_products.oz_sku`: INTEGER → BIGINT
- `oz_barcodes.oz_product_id`: INTEGER → BIGINT

### Wildberries Tables
- `wb_products.wb_sku`: INTEGER → BIGINT
- `wb_prices.wb_sku`: INTEGER → BIGINT

## Solution Implementation

### 1. Schema Updates
Updated `utils/db_schema.py` to define all SKU and Product ID fields as `BIGINT`:

```python
{'target_col_name': 'wb_sku', 'sql_type': 'BIGINT', 'source_col_name': 'Артикул WB', 'notes': None}
{'target_col_name': 'oz_product_id', 'sql_type': 'BIGINT', 'source_col_name': 'Ozon Product ID', 'notes': None}
```

### 2. Migration System
Added automatic migration detection and execution in `utils/db_migration.py`:

- `check_migration_needed()` - Detects Ozon tables needing migration
- `check_wb_sku_migration_needed()` - Detects Wildberries tables needing migration
- `migrate_integer_to_bigint_tables()` - Migrates Ozon tables
- `migrate_wb_sku_to_bigint()` - Migrates Wildberries tables
- `auto_migrate_if_needed()` - Automatic detection and user-friendly migration interface

### 3. Import Page Integration
Updated `pages/2_🖇_Импорт_Отчетов_МП.py` to automatically check for and offer migration before import operations.

### 4. Database Schema File
Updated `project_schema.sql` to reflect the new BIGINT types for consistency.

## Migration Process

### Automatic Migration
1. When users visit the Import Reports page, the system automatically detects if migration is needed
2. If needed, migration buttons are displayed with detailed explanations
3. Users can click to execute the migration, which:
   - Drops existing tables with INTEGER fields
   - Recreates them with BIGINT fields
   - Prompts users to re-import their data

### Manual Migration
Users can also trigger migration manually by running:
```python
from utils.db_migration import auto_migrate_if_needed
from utils.db_connection import connect_db

conn = connect_db()
auto_migrate_if_needed(conn)
```

## Testing

A test script `test_bigint_migration.py` is provided to verify the migration works correctly with large values:

```bash
python test_bigint_migration.py
```

The test verifies:
- Import of values larger than INT32_MAX (2,147,483,647)
- Correct storage and retrieval of large values
- Both Ozon and Wildberries table functionality

## Data Range Support

After migration, the system supports:
- **INTEGER range**: -2,147,483,648 to 2,147,483,647
- **BIGINT range**: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807

This provides sufficient range for all current and future marketplace identifier values.

## User Impact

### Before Migration
- Import failures with "Value out of range for type INT" errors
- Unable to import data with large SKU/Product ID values

### After Migration
- Seamless import of all marketplace data
- Support for large identifier values
- Automatic detection and user-friendly migration process
- No data loss (users re-import after migration)

## Quick Fix Instructions

If you're encountering the "Value out of range for type INT" error when importing Ozon Barcodes:

### Option 1: Automatic Migration (Recommended)
1. Go to the **Import Reports** page (`pages/2_🖇_Импорт_Отчетов_МП.py`)
2. The system will automatically detect if migration is needed
3. Click the migration button that appears at the top of the page
4. After migration completes, re-import your data

### Option 2: Manual Migration
1. Close any running Streamlit applications
2. Run the migration script:
   ```bash
   python run_migration.py
   ```
3. Follow the prompts to complete migration
4. Restart your Streamlit application and re-import data

### Option 3: Check Current Status
To check if your database needs migration:
```bash
python check_db_schema.py
```

This will show you the current data types for all SKU fields.

## Files Modified

1. `utils/db_schema.py` - Updated schema definitions
2. `utils/db_migration.py` - Added migration functions
3. `pages/2_🖇_Импорт_Отчетов_МП.py` - Integrated migration checks
4. `project_schema.sql` - Updated schema documentation
5. `test_bigint_migration.py` - Added test verification
6. `docs/bigint_migration_fix.md` - This documentation

## Future Considerations

- The migration system can be extended for future schema changes
- All new tables should use BIGINT for identifier fields by default
- Consider adding data validation to warn about potential overflow issues