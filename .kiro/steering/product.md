---
inclusion: always
---

# DataFox SL - Marketplace Analytics Platform

## Domain Conventions

### Marketplace Terminology
- **Ozon (OZ)**: Use `oz_` prefix for all Ozon-related tables, variables, and functions
- **Wildberries (WB)**: Use `wb_` prefix for all Wildberries-related tables, variables, and functions
- **Cross-marketplace**: Use `marketplace_links` table for product relationships between platforms
- **Barcodes**: Primary linking mechanism between marketplaces - always validate barcode format and uniqueness

### Data Handling Patterns
- **Product Cards**: Always include `product_id`, `barcode`, `title`, `price`, `category` as minimum fields
- **Cross-linking**: Use `CrossMarketplaceLinker` class for all product relationship operations
- **Data Import**: Validate marketplace data format before processing, handle encoding issues (Russian text)
- **Excel Processing**: Use `openpyxl` for complex operations, `pandas` for simple read/write operations

## Architecture Patterns

### Streamlit Page Structure
- Use numbered prefixes for page ordering: `1_🏠_`, `2_🖇_`, etc.
- Include emoji indicators for visual navigation
- Implement progress bars for long-running operations using `st.progress()`
- Use `@st.cache_data` for expensive data operations with 300-second TTL

### Database Operations
- **DuckDB**: Primary analytical database for aggregations and complex queries
- **PostgreSQL**: TypeScript service persistence only
- Always use parameterized queries to prevent SQL injection
- Implement connection pooling for concurrent operations

### Error Handling
- Use `st.error()`, `st.warning()`, `st.info()` for user feedback
- Log marketplace API errors with context (marketplace, operation, timestamp)
- Implement graceful degradation for missing marketplace data

## Cross-Marketplace Rules

### Product Linking
- Barcodes are the primary linking mechanism - validate format before processing
- Support bidirectional linking (OZ→WB and WB→OZ)
- Handle duplicate barcodes by prioritizing most recent data
- Maintain link confidence scores based on data quality

### Data Synchronization
- Always check for existing links before creating new ones
- Update link timestamps on data refresh
- Handle marketplace-specific data formats and encoding differences
- Preserve original marketplace data alongside normalized versions

## UI/UX Conventions

### Streamlit Components
- Use consistent sidebar navigation with marketplace filters
- Implement data tables with sorting and filtering capabilities
- Show processing progress for operations >2 seconds
- Use expandable sections for detailed configuration options

### Russian Language Support
- Handle Cyrillic text encoding properly in all data operations
- Use Russian labels in UI while maintaining English code comments
- Support both Russian and English search terms in product matching

## Performance Considerations

### Data Processing
- Batch process large datasets (>10k records) with progress indicators
- Use DuckDB for analytical queries, avoid loading large datasets into memory
- Implement pagination for large result sets in UI
- Cache frequently accessed marketplace data

### Memory Management
- Stream large Excel files instead of loading entirely into memory
- Use generators for processing large product catalogs
- Clear Streamlit cache when switching between large datasets

## Security & Data Handling

### Marketplace Data
- Never expose API keys or credentials in logs or UI
- Sanitize user inputs for database queries and file operations
- Validate file uploads for size and format before processing
- Handle PII in marketplace data according to data protection requirements