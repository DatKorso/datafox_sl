# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**DataFox SL** is a Streamlit-based marketplace analytics platform specializing in Ozon and Wildberries data analysis. The platform focuses on cross-marketplace product linking, advanced analytics, and Rich Content generation for marketplace optimization.

## Quick Start Commands

```bash
# Install dependencies (UV package manager preferred)
uv install
# Or with pip
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
- Database: DuckDB files in `data/` directory (auto-created, gitignored)
- Configuration: Copy `config.example.json` to `config.json`
- Schema: Managed through `utils/db_schema.py` with auto-migration
- Performance: Connection pooling via `@st.cache_resource`
- Package Management: UV (pyproject.toml) preferred over pip (requirements.txt)

## High-Level Architecture

**Core:** Streamlit-based marketplace analytics platform for Ozon and Wildberries data analysis.

### Application Architecture
- **Entry Point**: `app.py` serves as the main Streamlit application with sidebar navigation
- **Page System**: 17 feature pages in `pages/` directory with emoji-prefixed numbering (1-17)
- **Core Logic**: Centralized business logic in `utils/` with 25+ specialized modules
- **Data Storage**: DuckDB database with auto-migration and connection pooling

### Critical Cross-Marketplace Linking System
**The foundation of all analytics features:**

```python
# Central linking class used across all features
class CrossMarketplaceLinker:
    def _normalize_and_merge_barcodes()  # Barcode normalization & joining
    def find_linked_products()           # Main WB ↔ Ozon linking method
```

**Barcode Linking Flow:**
1. **Normalization**: Strip spaces, validate EAN-13, position-based priority (first = primary)
2. **Cross-Matching**: Join `wb_products.barcodes` ↔ `oz_barcodes.barcode` 
3. **Data Enrichment**: Merge linked products with marketplace-specific attributes
4. **Position Tracking**: Preserve barcode position for priority-based selection

### Core Engine Classes

**WB Recommendation System** (`utils/wb_recommendations.py`):
```python
class WBRecommendationEngine:
    def get_recommendations()  # Similarity-based product matching
    def _calculate_similarity_score()  # Multi-factor scoring algorithm

class WBProductInfo:  # Data model with Ozon enrichment
```

**Rich Content Generation** (`utils/rich_content_oz.py`):
```python
class RichContentProcessor:
    def process_batch_optimized()  # Memory-safe batch processing
    def _generate_rich_content_json()  # Ozon Rich Content JSON creation
```

**Advanced Product Grouping** (`utils/advanced_product_grouper.py`):
```python
class AdvancedProductGrouper:
    def create_groups()  # Multi-criteria product clustering
```

### Data Flow Architecture
```
Excel/CSV Reports → Import Pages (2) → DuckDB Storage → CrossMarketplaceLinker → 
Analytics Pages (5-17) → Export/Reports
```

**Database Schema Design:**
- **WB Tables**: `wb_products` (characteristics), `wb_prices` (pricing/stock)
- **Ozon Tables**: `oz_products` (master), `oz_barcodes` (linking), `oz_category_products` (enriched), `oz_orders` (transactions), `oz_card_rating` (quality)
- **Linking Strategy**: Normalized barcode matching with position-based prioritization

### Session State & Caching Strategy
- **Connection Pooling**: `@st.cache_resource` for database connections
- **Data Caching**: `@st.cache_data` with TTL for expensive computations  
- **Session Management**: Minimal session state usage, aggressive cleanup after operations
- **Memory Safety**: Batch processing for >1000 records, streaming exports >50MB

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

### Critical Development Rules

**ALWAYS use `CrossMarketplaceLinker` for any WB↔Ozon connections:**
```python
from utils.cross_marketplace_linker import CrossMarketplaceLinker
linker = CrossMarketplaceLinker(db_conn)
linked_products = linker.find_linked_products(oz_vendor_codes=codes)
```

**Memory Management (Critical for Streamlit):**
- Clear session state after heavy operations: `st.session_state.clear()`
- Use batch processing for >1000 records with progress callbacks
- Implement streaming for exports >50MB: `yield chunk.to_csv()`

**Database Connection Pattern:**
```python
@st.cache_resource
def get_database_connection():
    return connect_db()
```

**Error Handling with User Context:**
```python
try:
    result = process_data()
except Exception as e:
    st.error(f"Operation failed: {e}")
    st.info("Suggested fix: Check data format and try again.")
```

### Architecture Dependencies (Must understand before development)

**Cross-Marketplace Foundation:**
- `cross_marketplace_linker.py` → Used by all analytics features (pages 5-16)
- `db_search_helpers.py` → Barcode normalization utilities for linking
- `data_cleaning.py` → Data validation and cleaning for all imports

**Core Business Logic:**
- `wb_recommendations.py` + `manual_recommendations_manager.py` → WB similarity engine (page 16)
- `rich_content_oz.py` + `rich_content_oz_ui.py` → Ozon content generation (page 11)
- `advanced_product_grouper.py` → Multi-criteria clustering (page 14)
- `analytic_report_helpers.py` → Excel report generation (page 8)

**Database Layer:**
- `db_connection.py` → Connection pooling (used by ALL features)
- `db_schema.py` → Schema management & migrations
- `db_migration.py` → Version tracking & auto-migration

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

**CrossMarketplaceLinker:**
```python
from utils.cross_marketplace_linker import CrossMarketplaceLinker

linker = CrossMarketplaceLinker(db_conn)
linked_products = linker.find_linked_products(
    oz_vendor_codes=['CODE1', 'CODE2'], include_wb_data=True)
```

### WB Recommendations Engine

**WB Recommendations:**
```python
from utils.wb_recommendations import WBRecommendationEngine
from utils.manual_recommendations_manager import ManualRecommendationsManager

engine = WBRecommendationEngine(db_conn, config)
manual_manager = ManualRecommendationsManager()
manual_manager.load_from_csv(manual_recommendations_path)

recommendations = engine.get_recommendations(
    wb_sku="123456", count=20, manual_manager=manual_manager)
```

### Advanced Product Grouping

**Advanced Product Grouping:**
```python
from utils.advanced_product_grouper import AdvancedProductGrouper, GroupingConfig

config = GroupingConfig(
    grouping_columns=['oz_brand', 'oz_category'], min_group_rating=4.0,
    max_wb_sku_per_group=5, enable_sort_priority=True)
    
grouper = AdvancedProductGrouper(db_conn)
result = grouper.create_groups(config)
```

### Rich Content Generation

**Rich Content Generation:**
```python
from utils.rich_content_oz import RichContentProcessor

processor = RichContentProcessor(db_conn, config)
result = processor.process_batch_optimized(
    vendor_codes, batch_size=50, processing_mode="memory_safe")
```

### Database Query Patterns

**Database Queries:**
```python
query = """
SELECT ocp.oz_vendor_code, op.oz_sku, op.oz_fbo_stock
FROM oz_category_products ocp
LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
WHERE ocp.oz_brand = ? AND op.oz_fbo_stock > ?
"""
results = conn.execute(query, [brand, min_stock]).fetchall()
```

### Streamlit UI Patterns

**Progress & Memory Management:**
```python
# Progress callback
def progress_callback(current, total, message):
    progress_bar.progress(current / total)
    status_text.text(f"{current}/{total}: {message}")

with st.spinner("Processing..."):
    result = processor.process_batch(codes, progress_callback)

# Memory cleanup
if 'heavy_data' in st.session_state:
    del st.session_state.heavy_data
    st.rerun()
```

### Data Processing Patterns

**External Services:**
```python
# Google Sheets
from utils.google_sheets_utils import read_google_sheets_as_dataframe
df = read_google_sheets_as_dataframe(sheets_url)

# WB Photo Service
from utils.wb_photo_service import get_wb_photo_url, validate_wb_photo_url
photo_url = get_wb_photo_url("123456")
is_valid, message = validate_wb_photo_url("123456")

# Database Migration
from utils.db_migration import auto_migrate_if_needed
if auto_migrate_if_needed(db_conn):
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

### Common Issues & Quick Solutions

| Issue | Solution | Code |
|-------|----------|------|
| **WebSocket fails** | Stream exports | `def generate_export_stream(): for chunk in process_in_chunks(data): yield chunk.to_csv()` |
| **Session state too large** | Clear regularly | `if len(st.session_state.get('results', [])) > 1000: st.session_state.clear()` |
| **Slow linking** | Batch operations | `linker._normalize_and_merge_barcodes(oz_vendor_codes=vendor_codes)` |
| **Schema outdated** | Auto-migrate | `auto_migrate_if_needed(db_conn)` |

| **Poor WB recommendations** | Use manual overrides | `manual_manager = ManualRecommendationsManager(); manual_manager.load_from_csv("file.csv")` |
| **Google Sheets errors** | Validate URLs | `validate_google_sheets_url(url); diagnose_google_sheets_encoding(url)` |

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

**Data & Testing:**
- Test with real data from `marketplace_reports/ozon/CustomFiles/`
- Validate cross-marketplace linking with real barcode data
- Test WB recommendations across product categories
- Test manual recommendations integration and overrides

**Performance & Memory:**
- Monitor memory usage during development
- Use batch processing for >1000 records
- Clear session state after heavy operations
- Implement progress tracking for long operations

**Integration & Quality:**
- Run database migrations when needed
- Use proper error handling for external integrations
- Validate Google Sheets URLs and Rich Content JSON
- Follow existing patterns and document new ones

## Module Guidelines

| Module | Key Practices |
|--------|--------------|
| **WB Recommendations** | Validate SKU format, use manual overrides, cache results, monitor quality |
| **Advanced Grouper** | Configure parameters per business needs, monitor compensation effectiveness |
| **Data Cleaning** | Apply consistent rules, validate integrity, handle edge cases gracefully |
| **Google Sheets** | Validate URLs, handle encoding proactively, implement timeouts |

This project handles sensitive marketplace data - ensure security, performance, and data integrity are maintained at all times.