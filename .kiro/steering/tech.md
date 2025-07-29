# Technology Stack & Build System

## Primary Stack

### Python Application (Main Interface)
- **Framework**: Streamlit 1.45.1 for web interface
- **Database**: DuckDB 1.3.0 (embedded analytical database)
- **Data Processing**: Pandas 2.2.3, openpyxl 3.1.5 for Excel handling
- **Visualization**: Plotly 6.1.2 for interactive charts
- **HTTP**: requests 2.32.3 for external API calls

### TypeScript Microservice (rich_recommend)
- **Runtime**: Node.js with TypeScript
- **Framework**: Express.js for REST API
- **Database**: PostgreSQL with TypeORM
- **File Processing**: multer for uploads, xlsx for Excel processing
- **Architecture**: Modular design with controllers, services, entities

## Configuration

### Database Configuration
- **Main DB**: DuckDB file at `data/marketplace_data.db`
- **Config**: JSON-based configuration in `config.json`
- **Schema**: Centralized schema management in `utils/db_schema.py`

### Environment Setup
- **Python**: Requires Python 3.8+
- **Dependencies**: Install via `pip install -r requirements.txt`
- **TypeScript Service**: Separate Node.js service with own dependencies

## Common Commands

### Python Application
```bash
# Install dependencies
pip install -r requirements.txt

# Run main application
streamlit run app.py

# Run tests
pytest tests/
```

### TypeScript Service
```bash
# Navigate to service directory
cd rich_recommend

# Install dependencies
npm install

# Run development server
npm run dev

# Build for production
npm run build
```

### Database Operations
```bash
# Database connection test (via Python)
python -c "from utils.db_connection import test_db_connection; print(test_db_connection())"

# Schema export
python utils/export_db_schema.py
```

## Development Patterns

### Error Handling
- Use try-catch blocks with specific error messages
- Streamlit error display: `st.error()`, `st.warning()`, `st.info()`
- Progress tracking with `st.progress()` and custom progress managers

### Data Processing
- Pandas for data manipulation and cleaning
- DuckDB for analytical queries and aggregations
- Centralized data cleaning utilities in `utils/data_cleaning.py`

### Caching
- Streamlit caching: `@st.cache_data` for data operations
- TTL-based caching (typically 300 seconds) for expensive operations
- Resource caching: `@st.cache_resource` for database connections