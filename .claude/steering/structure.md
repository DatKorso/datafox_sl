# Project Structure - DataFox SL

## Directory Architecture

### Root Level Organization
```
datafox_sl/
├── app.py                    # Main Streamlit application entry point
├── requirements.txt          # Python dependencies with pinned versions
├── config.example.json       # Example configuration template
├── config.json              # User configuration (gitignored)
├── CLAUDE.md                # Claude Code development instructions
├── README.md                # Project overview and quick start
├── pages/                   # Streamlit pages (feature modules)
├── utils/                   # Core utility modules and business logic
├── data/                    # Database files and local data storage
├── project-docs/            # Comprehensive project documentation
├── marketplace_reports/     # Input data from marketplaces
├── tests/                   # Test suite with fixtures
├── rich_recommend/          # TypeScript/Node.js auxiliary services
└── venv/                    # Python virtual environment (gitignored)
```

### Page Module Organization
**Convention**: Emoji-prefixed numbering system for logical grouping
```
pages/
├── 1_🏠_Главная.py                    # Home dashboard
├── 2_🖇_Импорт_Отчетов_МП.py          # Data import functionality
├── 3_⚙️_Настройки.py                  # Application settings
├── 4_📖_Просмотр_БД.py                # Database viewer
├── 5_🔎_Поиск_Между_МП.py             # Cross-marketplace search
├── 6-9_📊_*.py                       # Analytics and reporting tools
├── 10-12_🚧_*.py                     # Advanced processing tools
├── 13-17_🎯📊_*.py                   # Specialized utilities
```

**Page Category Groups:**
- **🏠 Core Navigation** (1-4): Essential app functions and data management
- **🔎📊 Analytics** (5-9): Search, statistics, and reporting tools  
- **🚧 Advanced Processing** (10-12): Complex data processing and content generation
- **🎯📊 Specialized Tools** (13-17): Recommendations, Excel tools, and utilities

### Utility Module Architecture
```
utils/
├── Core Data Management
│   ├── db_connection.py              # Database connection pooling
│   ├── db_schema.py                  # Schema management and setup
│   ├── db_migration.py               # Auto-migration system
│   ├── db_cleanup.py                 # Database optimization utilities
│   ├── config_utils.py               # Configuration management
│   └── data_cleaning.py              # Data validation and cleaning
├── Cross-Marketplace Intelligence
│   ├── cross_marketplace_linker.py   # Core WB ↔ Ozon linking system
│   ├── db_search_helpers.py          # Search algorithms and queries
│   └── oz_to_wb_collector.py         # Cross-marketplace data collection
├── Business Logic Modules
│   ├── wb_recommendations.py         # WB recommendation engine
│   ├── manual_recommendations_manager.py # Manual recommendation overrides
│   ├── advanced_product_grouper.py   # Product grouping with compensation
│   ├── rich_content_oz.py            # Rich Content generation engine
│   └── wb_photo_service.py           # WB image URL service
├── Analytics & Reports
│   ├── analytic_report_helpers.py    # Excel analytics processing
│   ├── cards_matcher_helpers.py      # Card matching algorithms
│   └── category_helpers.py           # Category comparison utilities
├── UI & Integration
│   ├── rich_content_oz_ui.py         # Rich Content UI components
│   ├── advanced_grouping_ui_components.py # Advanced grouping UI
│   ├── google_sheets_utils.py        # Google Sheets integration
│   └── theme_utils.py                # UI theming utilities
└── Development & Maintenance
    ├── export_db_schema.py           # Schema export utilities
    ├── emergency_rich_content_export.py # Memory-safe export tools
    └── wb_recommendations_debug.py   # Debugging utilities
```

## Naming Conventions

### File Naming Standards
- **Pages**: `{number}_{emoji}_{Russian_Name}.py`
  - Sequential numbering for logical organization
  - Emoji for visual category identification
  - Russian names for user-facing functionality
- **Utils**: `{functional_domain}_{specific_purpose}.py`
  - Descriptive English names
  - Underscore separation for readability
  - Domain-specific prefixes (db_, wb_, oz_, etc.)

### Code Organization Patterns
- **Class Names**: PascalCase (`CrossMarketplaceLinker`, `WBRecommendationEngine`)
- **Function Names**: snake_case (`get_linked_products`, `process_batch_optimized`)
- **Constants**: UPPER_SNAKE_CASE (`MAX_BATCH_SIZE`, `DEFAULT_CONFIG_PATH`)
- **Private Methods**: Leading underscore (`_normalize_barcodes`, `_validate_input`)

### Database Conventions
- **Table Names**: `{marketplace_prefix}_{entity}` (e.g., `oz_products`, `wb_prices`)
- **Column Names**: `{marketplace_prefix}_{attribute}` (e.g., `oz_sku`, `wb_vendor_code`)
- **Linking Tables**: Clear relationship indicators (`oz_barcodes`, `cross_marketplace_links`)

## Development Patterns

### Streamlit Application Patterns
- **Page Structure**: Self-contained modules with clear entry points
- **Session State**: Minimize usage, clear heavy objects after operations
- **Caching Strategy**: 
  - `@st.cache_data` for DataFrame operations and data transformations
  - `@st.cache_resource` for database connections and heavy objects
- **Error Handling**: User-friendly error messages with technical logging

### Data Processing Patterns
- **Cross-Marketplace Operations**: Always use `CrossMarketplaceLinker` for WB ↔ Ozon connections
- **Batch Processing**: Default for operations >1000 records with progress callbacks
- **Memory Management**: Generators for large exports, streaming for files >50MB
- **Database Queries**: Parameterized queries with proper error handling

### Configuration Management
- **Settings Hierarchy**: config.json → environment variables → defaults
- **Validation**: Input validation with user-friendly error messages
- **Persistence**: Configuration changes saved automatically
- **Security**: Sensitive data .gitignored, no hardcoded credentials

## Testing Architecture

### Test Organization
```
tests/
├── conftest.py                       # Shared fixtures and configuration
├── unit/                            # Unit tests for specific modules
│   ├── test_rich_content_processor.py
│   ├── test_recommendation_engine.py
│   ├── test_data_collector.py
│   └── test_*.py
└── integration/                     # Integration tests (future)
```

### Testing Standards
- **Framework**: pytest with comprehensive fixtures
- **Coverage Focus**: Critical business logic and data processing
- **Mock Strategy**: Database fixtures for isolated testing
- **Test Data**: Sample files in `marketplace_reports/ozon/CustomFiles/`

## Documentation Structure

### Documentation Hierarchy
```
project-docs/
├── overview.md                      # Project overview and capabilities
├── requirements.md                  # Business requirements
├── tech-specs.md                   # Technical specifications
├── user-guides/                    # End-user documentation
│   ├── feature-specific guides
├── technical/                      # Technical documentation
│   ├── architecture-overview.md
│   ├── implementation/             # Implementation guides
│   └── api/                       # API documentation
└── project-management/             # Project planning and reports
```

### Documentation Standards
- **Markdown Format**: All documentation in markdown for version control
- **Russian + English**: User guides in Russian, technical docs in English
- **Code Examples**: Working examples with real data scenarios
- **Versioning**: Document updates aligned with feature releases

## Data Architecture

### Database Organization
- **Core Tables**: Marketplace-specific product and pricing data
- **Linking Tables**: Cross-marketplace relationships via barcodes
- **Configuration Tables**: User settings and application state
- **Temporary Tables**: Processing intermediate results

### File System Organization
```
data/
├── database.duckdb                  # Main application database
├── marketplace_data.db             # Legacy database (migration target)
└── backups/                        # Automatic backups with timestamps

marketplace_reports/
├── ozon/                           # Ozon marketplace data
│   ├── oz_products.csv
│   ├── oz_category_products/
│   └── CustomFiles/               # User-specific analysis files
└── wb/                            # Wildberries marketplace data
    ├── products/
    └── wb_recomm/                 # Recommendation data
```

## Integration Patterns

### External Service Integration
- **Google Sheets**: URL-based access with encoding detection
- **Photo Services**: WB photo URL generation and validation
- **File Processing**: Excel/CSV with batch processing capabilities

### API Design Philosophy
- **File-Based**: No direct marketplace API dependencies
- **Local-First**: All processing occurs locally
- **Batch Operations**: Efficient processing of large datasets
- **Error Recovery**: Graceful handling of processing failures

## Security & Privacy Patterns

### Data Protection
- **Local Storage**: All sensitive data remains on user's machine
- **File Permissions**: Appropriate database and file access controls
- **Configuration Security**: Sensitive settings .gitignored
- **No External Transmission**: No automatic data uploads or sharing

### Development Security
- **Input Validation**: Comprehensive validation for all user inputs
- **SQL Injection Prevention**: Parameterized queries throughout
- **File Access**: Restricted to designated directories
- **Error Information**: No sensitive data in error messages

## Performance Optimization Patterns

### Database Performance
- **Connection Pooling**: Efficient DuckDB connection management
- **Query Optimization**: Indexed queries and efficient joins
- **Batch Processing**: Bulk operations for large datasets
- **Memory Management**: Appropriate data structure choices

### UI Performance
- **Progressive Loading**: Pagination for large result sets
- **Caching Strategy**: Intelligent use of Streamlit caching
- **Memory Cleanup**: Session state management and cleanup
- **Progress Feedback**: User feedback for long operations

## Future Structure Considerations

### Scalability Patterns
- **Modular Architecture**: Easy addition of new marketplace integrations
- **Plugin System**: Extensible analysis tools and features
- **Configuration Flexibility**: Support for different deployment scenarios
- **Upgrade Path**: Backward compatibility for data and configuration