---
inclusion: always
---

# DataFox SL - Marketplace Analytics Platform

## Naming Conventions (MANDATORY)

### Marketplace Prefixes
- **Ozon**: Always use `oz_` prefix for tables, variables, functions (e.g., `oz_products`, `oz_barcodes`)
- **Wildberries**: Always use `wb_` prefix for tables, variables, functions (e.g., `wb_products`, `wb_prices`)
- **Cross-marketplace**: Use `marketplace_links` table for product relationships

### Code Style
- **Python**: `snake_case` for functions/variables, `PascalCase` for classes
- **Streamlit pages**: `{number}_{emoji}_{Russian_Name}.py` format
- **Database tables**: Lowercase with underscores, marketplace prefix required
- **Constants**: `UPPER_SNAKE_CASE`

## Data Validation Rules

### Required Fields
- **Product records**: Must include `product_id`, `barcode`, `title`, `price`, `category`
- **Barcodes**: Primary linking mechanism - validate format and uniqueness before processing
- **Russian text**: Handle Cyrillic encoding properly, use UTF-8 throughout

### Database Operations
- **Primary DB**: DuckDB for analytics (`data/marketplace_data.db`)
- **TypeScript service**: PostgreSQL only
- **Queries**: Always use parameterized queries, never string concatenation
- **Connections**: Use `utils.db_connection.connect_db()` for DuckDB access

## Architecture Patterns

### Streamlit Pages
- Use `@st.cache_data(ttl=300)` for expensive operations
- Show `st.progress()` for operations >2 seconds
- Error feedback: `st.error()`, `st.warning()`, `st.info()`
- Sidebar navigation with marketplace filters

### Cross-Marketplace Linking
- Use `CrossMarketplaceLinker` class for all product relationships
- Support bidirectional linking (OZ↔WB)
- Check existing links before creating new ones
- Maintain confidence scores based on data quality

### File Processing
- **Excel**: Use `openpyxl` for complex operations, `pandas` for simple read/write
- **Large files**: Stream processing, avoid loading entire files into memory
- **Uploads**: Validate size and format before processing

## Error Handling Requirements

### User Feedback
- Always provide clear error messages in Russian for UI
- Log technical details in English with context (marketplace, operation, timestamp)
- Implement graceful degradation for missing marketplace data
- Use try-catch blocks with specific error types

### Data Processing
- Validate marketplace data format before processing
- Handle duplicate barcodes by prioritizing most recent data
- Preserve original data alongside normalized versions
- Batch process large datasets (>10k records) with progress indicators

## Performance Rules

### Memory Management
- Use generators for large product catalogs
- Clear Streamlit cache when switching between large datasets
- Implement pagination for large result sets
- Cache frequently accessed marketplace data

### Database Queries
- Use DuckDB for analytical queries, avoid loading large datasets into memory
- Implement connection pooling for concurrent operations
- Index on barcode fields for cross-marketplace linking

## Security Requirements

### Data Protection
- Never expose API keys or credentials in logs or UI
- Sanitize all user inputs for database queries and file operations
- Handle PII according to data protection requirements
- Validate file uploads for security threats