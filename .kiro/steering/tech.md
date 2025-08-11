---
inclusion: always
---

# Technology Stack & Development Patterns

## Architecture Rules

### Dual-Service Architecture
- **Python/Streamlit**: Main application for UI, analytics, and data processing
- **TypeScript/Node.js**: Microservice (`rich_recommend/`) for AI processing and complex operations
- **NEVER** mix database connections between services
- **ALWAYS** use HTTP API calls for Python ↔ TypeScript communication

### Database Usage Patterns
- **DuckDB** (`data/marketplace_data.db`): Primary analytical database for Python app
- **PostgreSQL**: TypeScript service only, managed via TypeORM
- **MUST** use `utils.db_connection.connect_db()` for all DuckDB access
- **ALWAYS** use parameterized queries, never string concatenation
- **REQUIRED** connection context managers for database operations

## Technology Selection Rules

### Python Stack (Main Application)
- **UI Framework**: Streamlit for all user interfaces
- **Data Processing**: Pandas for manipulation, DuckDB for analytics
- **Excel Operations**: openpyxl for complex operations, pandas for simple read/write
- **Visualization**: Plotly for interactive charts
- **HTTP Requests**: requests library for external APIs

### TypeScript Stack (Microservice)
- **Framework**: Express.js with TypeScript
- **Database**: PostgreSQL with TypeORM entities
- **File Processing**: multer for uploads, xlsx for Excel processing
- **Architecture**: Controller → Service → Entity pattern

## Development Workflow

### Essential Commands
```bash
# Python development
streamlit run app.py                    # Start main application
pytest tests/                          # Run test suite
python utils/export_db_schema.py       # Export database schema

# TypeScript service
cd rich_recommend && npm run dev       # Start development server
cd rich_recommend && npm run build     # Build for production
```

### Database Operations
- **Schema Management**: Centralized in `utils/db_schema.py`
- **Migrations**: Use `utils/db_migration.py` for schema changes
- **Testing**: `python -c "from utils.db_connection import test_db_connection; print(test_db_connection())"`

## Mandatory Development Patterns

### Error Handling
- **MUST** use try-catch blocks with specific error types
- **REQUIRED** Streamlit user feedback: `st.error()`, `st.warning()`, `st.info()`
- **ALWAYS** provide Russian error messages for UI, English for logs
- **MUST** implement graceful degradation for missing data

### Performance & Caching
- **REQUIRED** `@st.cache_data(ttl=300)` for expensive operations
- **MUST** use `@st.cache_resource` for database connections
- **ALWAYS** show `st.progress()` for operations >2 seconds
- **REQUIRED** streaming for files >10MB, batch processing for >10k records

### Data Processing Standards
- **MUST** validate file uploads before processing
- **REQUIRED** UTF-8 encoding for all Russian text
- **ALWAYS** use generators for large datasets
- **MUST** clean up temporary files after processing
- **REQUIRED** progress indicators for long operations

### Code Organization
- **Database utilities**: Extend existing `utils/db_*.py` modules
- **UI components**: Create `*_ui_components.py` files in `utils/`
- **Data cleaning**: Use centralized `utils/data_cleaning.py`
- **Configuration**: JSON-based in `config.json`