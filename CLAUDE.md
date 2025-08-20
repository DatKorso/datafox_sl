# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**DataFox SL** is a Streamlit-based marketplace analytics platform specializing in Ozon and Wildberries data analysis. The platform focuses on cross-marketplace product linking, advanced analytics, and Rich Content generation for marketplace optimization.

## Quick Start Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Start the Streamlit application (default port 8501)
streamlit run app.py

# Development mode with auto-reload
streamlit run app.py --server.runOnSave true

# Start with custom port
streamlit run app.py --server.port 8502

# Reset database schema (if needed)
python -c "from utils.db_schema import setup_database; setup_database()"
```

## Testing and Development Commands

```bash
# Run tests (pytest framework)
python -m pytest tests/                    # All tests
python -m pytest tests/unit/              # Unit tests only
python -m pytest tests/unit/test_rich_content_processor.py  # Single test file
python -m pytest -v -s tests/             # Verbose output with prints

# Database operations
python -c "from utils.db_connection import connect_db; print('DB Status:', connect_db() is not None)"
python -c "from utils.db_migration import auto_migrate_if_needed, connect_db; auto_migrate_if_needed(connect_db())"
python -c "from utils.db_cleanup import get_cleanup_recommendations, connect_db; print(get_cleanup_recommendations(connect_db()))"

# Rich Content processing (CLI)
python utils/export_rich_content.py       # Emergency export for large datasets
python utils/emergency_rich_content_export.py  # Memory-safe export

# WB Recommendations testing
python -c "from utils.wb_recommendations import WBRecommendationEngine; print('WB Recommendations OK')"
python -c "from utils.manual_recommendations_manager import ManualRecommendationsManager; print('Manual Recommendations OK')"

# Data validation and cleaning
python -c "from utils.cross_marketplace_linker import CrossMarketplaceLinker; print('Linker OK')"
python -c "from utils.data_cleaning import DataCleaningUtils; print('Data Cleaner OK')"
python -c "from utils.advanced_product_grouper import AdvancedProductGrouper; print('Advanced Grouper OK')"

# Google Sheets integration
python -c "from utils.google_sheets_utils import validate_google_sheets_url; print('Sheets Utils OK')"

# Photo service validation
python -c "from utils.wb_photo_service import get_wb_photo_url; print('Photo Service OK')"

# Performance testing
python -c "import duckdb; print('DuckDB Version:', duckdb.__version__)"
```

**Database & Config:**
- Database: DuckDB files in `data/` directory (auto-created)
- Configuration: Copy `config.example.json` to `config.json`
- Schema: Managed through `utils/db_schema.py` with auto-migration
- Performance: Connection pooling via `@st.cache_resource`
- Streamlit config: `.streamlit/config.toml` (optimized for large data processing)

## High-Level Architecture

**Core:** Streamlit-based marketplace analytics platform for Ozon and Wildberries data analysis.

**Key Components:**
- **App (`app.py`)**: Multi-page Streamlit application with sidebar navigation
- **Pages (`pages/`)**: Standalone modules with emoji-prefixed numbering for specific functionality
- **Utils (`utils/`)**: Core utilities including `CrossMarketplaceLinker` for barcode-based product linking between marketplaces

**Cross-Marketplace Linking:**
- Products linked between WB ↔ Ozon via shared barcodes
- `CrossMarketplaceLinker` class handles normalization, position tracking, and primary barcode identification
- Critical for all cross-platform analysis features

**Data Flow:**
1. Import marketplace reports (Excel/CSV) → DuckDB tables
2. Cross-marketplace linking via `CrossMarketplaceLinker`
3. Analytics and export with integrated data

**Database Schema:**
- **WB**: `wb_products`, `wb_prices`
- **Ozon**: `oz_products`, `oz_barcodes`, `oz_category_products`, `oz_card_rating`, `oz_orders`
- **Linking**: Via normalized barcodes with position tracking

**Key Features:**
- Cross-marketplace search and product linking via barcodes
- WB product recommendation engine with manual overrides
- Rich Content generation for Ozon marketplace optimization
- Advanced product grouping with rating compensation algorithms
- Analytics and reporting with Excel integration
- Database cleanup and optimization tools
- Google Sheets integration for external data sources
- Memory-safe batch processing for large datasets

## Development Guidelines

### Code Organization Best Practices

**Streamlit Architecture:**
- **Page Modules**: Self-contained with emoji prefixes (e.g., `11_🚧_Rich_Контент_OZ.py`)
- **Shared Utilities**: Core logic in `utils/` directory
- **Caching Strategy**: Use `@st.cache_data` for DataFrames, `@st.cache_resource` for connections
- **Session State**: Minimize usage, clear heavy objects after operations

**Data Processing Patterns:**
- **Cross-Marketplace Linking**: Always use `CrossMarketplaceLinker` for WB↔Ozon connections
- **Barcode Normalization**: Position-based prioritization (first = primary)
- **Database Operations**: Pandas + DuckDB with connection pooling
- **Memory Management**: Use batch processing for >1000 records

### Performance Optimization

**Database Queries:**
- Use parameterized queries to prevent SQL injection
- Implement query result caching for repeated operations
- Batch operations when processing large datasets
- Use `LIMIT` clauses for UI previews

**Memory Management:**
- Clear session state after heavy operations: `st.session_state.clear()`
- Use generators for large data exports
- Implement streaming for CSV downloads >50MB
- Monitor memory usage with DuckDB connection pooling

**UI Responsiveness:**
- Use `st.spinner()` for long operations
- Implement progress bars for batch processing
- Add pagination for large result sets
- Use `st.empty()` containers for dynamic updates

### Testing Strategy

**Test Framework:** pytest with comprehensive fixtures in `tests/conftest.py`

**Data Validation:**
- Test with sample data in `marketplace_reports/ozon/CustomFiles/`
- Validate barcode linking between marketplaces
- Test Rich Content JSON generation and validation
- Verify database schema migrations

**Unit Testing:**
- Rich Content processor modules have comprehensive test coverage
- Mock database fixtures for isolated testing
- Test scoring algorithms and recommendation engines
- Validate data collection and processing logic

**Performance Testing:**
- Test with datasets >10K records
- Measure memory usage during batch operations
- Validate export functionality with large files
- Test concurrent user scenarios

**Error Handling:**
- Implement try-catch blocks for database operations
- Provide user-friendly error messages
- Log errors for debugging (use `logging` module)
- Graceful degradation for failed operations

### Security Considerations

**Data Protection:**
- Database files are .gitignored (sensitive marketplace data)
- Configuration files excluded from version control
- No hardcoded credentials in code
- Input validation for all user inputs

**Access Control:**
- Local-only application (no remote access)
- File system permissions for database directory
- Secure handling of marketplace API data

## Claude Code Development Workflow

### Core Development Process

**Role:** Expert data analytics engineer specializing in marketplace intelligence and Streamlit applications.

**Approach:** Proactive problem-solving with deep understanding of cross-marketplace data flows and performance optimization.

### Step-by-Step Development Process

**Step 1: Context Analysis**
```bash
# Always start with project context
ls -la utils/  # Check available utilities
grep -r "class.*Linker" utils/  # Find linking components
python -c "from utils.db_connection import connect_db; print('DB Ready')"
```

**Step 2: Task-Specific Development Patterns**

**Data Processing Tasks:**
1. Use `CrossMarketplaceLinker` for WB↔Ozon connections
2. Implement batch processing for >1000 records
3. Add progress tracking with `st.progress()` and callbacks
4. Use `@st.cache_data` for expensive computations
5. Clear session state after heavy operations

**UI Development:**
1. Follow emoji-prefixed page naming convention
2. Use `st.columns()` for responsive layouts
3. Implement `st.spinner()` for long operations
4. Add `st.empty()` containers for dynamic updates
5. Use `st.expander()` for detailed information

**Database Operations:**
1. Use parameterized queries for security
2. Implement connection pooling with `@st.cache_resource`
3. Add error handling with user-friendly messages
4. Use transactions for data integrity
5. Monitor memory usage with DuckDB

**Performance Optimization:**
1. Profile memory usage during development
2. Use generators for large exports
3. Implement streaming for files >50MB
4. Add pagination for large result sets
5. Use lightweight data structures

**Step 3: Quality Assurance**
- Test with sample data in `marketplace_reports/ozon/CustomFiles/`
- Validate cross-marketplace linking accuracy
- Test memory performance with large datasets
- Verify export functionality works correctly
- Document any new patterns in `project-docs/`

### Development Anti-Patterns to Avoid

❌ **Don't:** Store large datasets in `st.session_state`
✅ **Do:** Use database storage with cached connections

❌ **Don't:** Process all data at once for large datasets
✅ **Do:** Use batch processing with progress indicators

❌ **Don't:** Create duplicate linking logic
✅ **Do:** Always use `CrossMarketplaceLinker` class

❌ **Don't:** Hardcode database paths or credentials
✅ **Do:** Use `config.json` and environment variables

❌ **Don't:** Ignore memory constraints in Streamlit
✅ **Do:** Implement memory-safe modes for large operations

### Critical File Dependencies

**Core Dependencies:**
- `utils/cross_marketplace_linker.py` - WB↔Ozon product linking
- `utils/db_connection.py` - Database connection management
- `utils/rich_content_oz.py` - Rich Content generation engine
- `utils/db_schema.py` - Database schema management
- `utils/config_utils.py` - Configuration management and persistence
- `utils/data_cleaning.py` - Data validation and cleaning utilities

**Advanced Components:**
- `utils/wb_recommendations.py` - WB product recommendation engine
- `utils/manual_recommendations_manager.py` - Manual recommendation management
- `utils/advanced_product_grouper.py` - Enhanced product grouping with rating compensation
- `utils/oz_to_wb_collector.py` - Cross-marketplace SKU collection
- `utils/wb_photo_service.py` - WB image URL generation and validation
- `utils/analytic_report_helpers.py` - Excel analytics processing
- `utils/google_sheets_utils.py` - Google Sheets integration
- `utils/db_migration.py` - Database schema migration utilities
- `utils/db_cleanup.py` - Database optimization and cleanup tools

**Page Dependencies:**
- Rich Content: `pages/11_🚧_Rich_Контент_OZ.py` + `utils/rich_content_oz_ui.py`
- WB Recommendations: `pages/16_🎯_Рекомендации_WB.py` + `utils/wb_recommendations.py`
- Advanced Grouping: `pages/14_🎯_Улучшенная_Группировка_Товаров.py` + `utils/advanced_product_grouper.py`
- Cards Matching: `pages/10_🚧_Склейка_Карточек_OZ.py` + `utils/cards_matcher_helpers.py`
- Search: `pages/5_🔎_Поиск_Между_МП.py` + `utils/db_search_helpers.py`
- Analytics: `pages/8_📋_Аналитический_Отчет_OZ.py` + `utils/analytic_report_helpers.py`
- Data Collection: `pages/13_🔗_Сбор_WB_SKU_по_Озон.py` + `utils/oz_to_wb_collector.py`
- Excel Tools: `pages/15_📊_Объединение_Excel.py`, `pages/17_📊_Дробление_Excel.py`

### Emergency Procedures

**Memory Issues:**
```python
# Clear session state
st.session_state.clear()

# Use memory-safe export
python utils/emergency_rich_content_export.py
```

**Database Issues:**
```python
# Reset database schema
from utils.db_schema import setup_database
setup_database()
```

**Performance Issues:**
```python
# Check connection status
from utils.db_connection import connect_db
conn = connect_db()
print(f"Connection: {conn}")
```

## Project-Specific Patterns

### Cross-Marketplace Linking

**Always Use CrossMarketplaceLinker:**
```python
from utils.cross_marketplace_linker import CrossMarketplaceLinker

# Initialize with database connection
linker = CrossMarketplaceLinker(db_conn)

# Link products via barcodes
linked_products = linker.find_linked_products(
    oz_vendor_codes=['CODE1', 'CODE2'],
    include_wb_data=True
)
```

### WB Recommendations Engine

**For Product Recommendations:**
```python
from utils.wb_recommendations import WBRecommendationEngine
from utils.manual_recommendations_manager import ManualRecommendationsManager

# Initialize recommendation engine
engine = WBRecommendationEngine(db_conn, config)

# Setup manual recommendations (optional)
manual_manager = ManualRecommendationsManager()
manual_manager.load_from_csv(manual_recommendations_path)

# Get recommendations for WB SKU
recommendations = engine.get_recommendations(
    wb_sku="123456",
    count=20,
    manual_manager=manual_manager
)
```

### Advanced Product Grouping

**For Enhanced Grouping with Rating Compensation:**
```python
from utils.advanced_product_grouper import AdvancedProductGrouper, GroupingConfig

# Configure grouping parameters
config = GroupingConfig(
    grouping_columns=['oz_brand', 'oz_category'],
    min_group_rating=4.0,
    max_wb_sku_per_group=5,
    enable_sort_priority=True
)

# Initialize grouper
grouper = AdvancedProductGrouper(db_conn)

# Create groups with compensation
result = grouper.create_groups(config)
```

### Rich Content Generation

**For Large Datasets:**
```python
from utils.rich_content_oz import RichContentProcessor

# Use memory-safe mode for >1000 products
processor = RichContentProcessor(db_conn, config)
result = processor.process_batch_optimized(
    vendor_codes,
    batch_size=50,
    processing_mode="memory_safe"
)
```

### Database Query Patterns

**Efficient Cross-Table Queries:**
```python
# Use parameterized queries
query = """
SELECT ocp.oz_vendor_code, op.oz_sku, op.oz_fbo_stock
FROM oz_category_products ocp
LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
WHERE ocp.oz_brand = ? AND op.oz_fbo_stock > ?
"""
results = conn.execute(query, [brand, min_stock]).fetchall()
```

### Streamlit UI Patterns

**Progress Tracking:**
```python
# Use callback pattern for long operations
def progress_callback(current, total, message):
    progress_bar.progress(current / total)
    status_text.text(f"{current}/{total}: {message}")

# Apply to batch operations
with st.spinner("Processing..."):
    result = processor.process_batch(codes, progress_callback)
```

**Memory Management:**
```python
# Clear heavy objects after use
if 'heavy_data' in st.session_state:
    del st.session_state.heavy_data
    st.rerun()
```

### Data Processing Patterns

**Google Sheets Integration:**
```python
from utils.google_sheets_utils import read_google_sheets_as_dataframe

# Load data from Google Sheets
df = read_google_sheets_as_dataframe(sheets_url)
if df is not None:
    # Process the data
    pass
```

**WB Photo Service:**
```python
from utils.wb_photo_service import get_wb_photo_url, validate_wb_photo_url

# Get photo URL for WB SKU
photo_url = get_wb_photo_url("123456")

# Validate URL availability
is_valid, message = validate_wb_photo_url("123456")
```

**Database Migration:**
```python
from utils.db_migration import auto_migrate_if_needed

# Auto-migrate database schema if needed
migration_needed = auto_migrate_if_needed(db_conn)
if migration_needed:
    st.info("Database schema updated successfully")
```

### Error Handling Patterns

**Database Errors:**
```python
try:
    result = conn.execute(query).fetchall()
except Exception as e:
    st.error(f"Database error: {e}")
    st.info("Try refreshing the page or checking the database connection.")
    return None
```

**Memory Errors:**
```python
try:
    # Large operation
    result = process_large_dataset(data)
except MemoryError:
    st.error("Memory limit exceeded. Try using smaller batch sizes.")
    st.info("Consider using the 'memory_safe' mode for large operations.")
```

**Data Validation Errors:**
```python
from utils.data_cleaning import DataCleaningUtils

try:
    cleaner = DataCleaningUtils()
    cleaned_data = cleaner.apply_data_cleaning(df, table_name)
except ValueError as e:
    st.error(f"Data validation failed: {e}")
    st.info("Please check your data format and try again.")
```

## Common Issues & Solutions

### Issue: "WebSocket connection failed" during large exports
**Solution:** Use streaming export or pagination
```python
# Stream large exports
def generate_export_stream():
    for chunk in process_in_chunks(data):
        yield chunk.to_csv()

st.download_button(
    "Download",
    data=generate_export_stream(),
    file_name="export.csv"
)
```

### Issue: "Session state too large" error
**Solution:** Clear session state regularly
```python
# Clear after heavy operations
if len(st.session_state.get('results', [])) > 1000:
    st.session_state.clear()
    st.success("Memory cleared for better performance")
```

### Issue: Slow cross-marketplace linking
**Solution:** Use batch operations and caching
```python
# Batch barcode normalization
normalized_barcodes = linker._normalize_and_merge_barcodes(
    oz_vendor_codes=vendor_codes  # Process all at once
)
```

### Issue: Database schema out of date
**Solution:** Use auto-migration
```python
from utils.db_migration import auto_migrate_if_needed
auto_migrate_if_needed(db_conn)
```

### Issue: Poor WB recommendation quality
**Solution:** Use manual recommendations and tuning
```python
# Load manual recommendations to override algorithm
manual_manager = ManualRecommendationsManager()
manual_manager.load_from_csv("manual_recommendations.csv")

# Apply to recommendation engine
recommendations = engine.get_recommendations(
    wb_sku="123456",
    manual_manager=manual_manager
)
```

### Issue: Google Sheets access errors
**Solution:** URL validation and encoding detection
```python
from utils.google_sheets_utils import validate_google_sheets_url, diagnose_google_sheets_encoding

# Validate URL format
if not validate_google_sheets_url(url):
    st.error("Invalid Google Sheets URL format")

# Diagnose encoding issues
diagnosis = diagnose_google_sheets_encoding(url)
```

## Future Development Notes

**Planned Optimizations:**
- Implement Redis caching for frequent queries
- Add database indexing for better performance
- Create API endpoints for external integrations
- Add real-time data synchronization
- Optimize WB recommendation algorithms
- Enhance manual recommendation management UI
- Improve Google Sheets integration reliability

**Architecture Improvements:**
- Separate data processing from UI logic
- Implement proper logging throughout application
- Add comprehensive test suite for all utilities
- Create Docker deployment option
- Implement advanced caching strategies
- Add monitoring and alerting systems
- Create configuration management UI

---

## Important Reminders

1. **Always test with real data** from `marketplace_reports/ozon/CustomFiles/`
2. **Monitor memory usage** during development and production
3. **Use batch processing** for operations >1000 records
4. **Clear session state** after heavy operations
5. **Follow the existing patterns** in the codebase
6. **Document any new patterns** you create
7. **Test cross-marketplace linking** thoroughly with real barcode data
8. **Validate Rich Content JSON** generation and structure
9. **Test WB recommendations** with different product categories
10. **Validate Google Sheets integration** with various URL formats
11. **Run database migrations** when schema changes are needed
12. **Use proper error handling** for all external integrations
13. **Implement progress tracking** for long-running operations
14. **Test manual recommendations** integration and override logic

## Module-Specific Guidelines

### WB Recommendations (`utils/wb_recommendations.py`)
- Always validate WB SKU format before processing
- Use manual recommendations manager for quality control
- Implement proper caching for recommendation results
- Monitor recommendation quality metrics

### Advanced Grouper (`utils/advanced_product_grouper.py`)
- Configure grouping parameters based on business requirements
- Monitor compensation algorithm effectiveness
- Validate group quality before finalizing
- Use detailed logging for troubleshooting

### Data Cleaning (`utils/data_cleaning.py`)
- Apply consistent cleaning rules across all data sources
- Validate data integrity after cleaning operations
- Monitor cleaning effectiveness with metrics
- Handle edge cases gracefully

### Google Sheets Integration (`utils/google_sheets_utils.py`)
- Always validate URLs before attempting access
- Handle encoding issues proactively
- Implement proper timeout handling
- Provide clear error messages for failed operations

This project handles sensitive marketplace data - ensure security, performance, and data integrity are maintained at all times.