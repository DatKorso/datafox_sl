# Error Cards Analysis Page - Architecture Documentation

## Page Overview
**File**: `pages/12_🚨_Проблемы_Карточек_OZ.py`
**Purpose**: Analyze error field from oz_category_products table and provide filtering capabilities

## Database Schema Dependencies

### Primary Tables
1. **oz_category_products**: Main error analysis table
   - `oz_vendor_code`: Primary key for product identification
   - `error`: Semicolon-separated error messages
   - `warning`: Warning messages
   - `product_name`, `oz_brand`, `color`, `russian_size`: Product details
   - `oz_actual_price`, `oz_sku`: Pricing and SKU information

2. **oz_products**: Stock and inventory data
   - `oz_vendor_code`: Foreign key linking to oz_category_products
   - `oz_fbo_stock`: Current stock levels (key field for new functionality)

### JOIN Strategy
```sql
LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
```
- Uses LEFT JOIN to preserve all error records even if no stock data
- COALESCE ensures 0 stock for missing records

## Page Structure

### Tab 1: Error Analysis (Main Feature)
1. **Data Loading**: `load_error_data()` function with database JOIN
2. **Error Parsing**: `parse_error_types()` processes semicolon-separated errors
3. **Statistics Display**: Metrics and top error types analysis
4. **Filtering Section**: Multiple filter options including new stock filter
5. **Results Display**: Table, detailed view, and export options

### Tab 2: Field Discrepancy Analysis
- Universal tool for comparing parameter discrepancies
- WB SKU grouping and analysis
- External code analysis for vendor codes
- Color standardization tools

## Filtering Architecture

### Available Filters
1. **Error Types**: Multi-select from parsed error categories
2. **Brands**: Multi-select from available brands
3. **Price Range**: Slider-based price filtering
4. **Warnings**: Toggle to include warning messages
5. **Stock Filter**: NEW - Show only products with stock > 0

### Filter Application Order
1. Error type filtering (regex-based search)
2. Brand filtering (exact match)
3. Price range filtering (numerical comparison)
4. Warning inclusion (additional query + concatenation)
5. Stock filtering (numerical comparison: > 0)

## Key Functions

### Core Data Functions
- `load_error_data()`: Main data loading with stock JOIN
- `parse_error_types()`: Error message parsing and counting
- `find_field_discrepancies_for_wb_skus()`: Advanced discrepancy analysis

### Display Functions
- Table view with configurable columns
- Detailed expandable cards
- Export functionality (CSV format)
- Statistics and metrics visualization

## Performance Considerations
- `@st.cache_data` used for expensive database operations
- Session state management for analysis results
- Batch processing for large datasets
- Memory-safe export for large result sets

## Integration Points
- Uses `utils.config_utils` for database path configuration
- Integrates with `utils.db_connection` for database access
- Follows project patterns for error handling and user feedback
- Maintains consistency with other analysis pages

## Enhancement Patterns
The stock filtering enhancement follows established patterns:
1. UI element added to existing filter section
2. Database query enhanced with additional JOIN
3. Filter logic integrated with existing filter chain
4. Display configuration updated to include new column
5. Session state maintained for all filtering combinations