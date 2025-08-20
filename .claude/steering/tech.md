# Technology Stack - DataFox SL

## Core Technology Architecture

### Application Framework
- **Primary Framework**: Streamlit 1.45.1
  - Web-based interface with desktop application feel
  - Multi-page architecture using Streamlit's native page system
  - Session state management for user workflow continuity
  - Built-in caching decorators for performance optimization

### Database & Data Processing
- **Database**: DuckDB 1.3.0
  - High-performance analytics database optimized for large datasets
  - In-process database engine (no separate server required)
  - Excellent performance for OLAP workloads and analytical queries
  - Native integration with Pandas DataFrames

- **Data Processing**: Pandas 2.2.3
  - Primary data manipulation and transformation library
  - Integration with DuckDB for seamless data pipeline
  - Memory-efficient operations for large marketplace reports
  - Support for complex cross-marketplace data joining

### File Handling & Integration
- **Excel Processing**: OpenPyXL 3.1.5
  - Read/write Excel files from marketplace reports
  - Support for large files (up to 200MB)
  - Batch processing capabilities for memory efficiency

- **External Integration**: 
  - Google Sheets API integration for external reference data
  - Requests 2.32.3 for HTTP operations and marketplace photo services
  - Custom URL validation and encoding detection

### Visualization & UI
- **Charts & Analytics**: Plotly 6.1.2
  - Interactive charts and dashboards
  - Performance metrics visualization
  - Export capabilities for reports

- **UI Enhancement**: Streamlit-extras
  - Additional UI components and functionality
  - Enhanced user experience elements

- **Image Processing**: Pillow 11.2.1
  - Image handling for marketplace product photos
  - Support for WB photo service integration

### Development & Monitoring
- **File Watching**: Watchdog
  - Development mode file watching
  - Auto-reload capabilities during development

## Technical Decisions & Rationale

### Database Choice: DuckDB
**Why DuckDB over alternatives:**
- **Performance**: Excellent for analytical workloads on large datasets
- **Simplicity**: No server setup or maintenance required
- **Integration**: Native Pandas integration for seamless data flow
- **Memory Efficiency**: Optimized for processing large marketplace reports
- **Local-First**: Aligns with single-user, local deployment model

### Web Framework: Streamlit
**Why Streamlit over alternatives:**
- **Rapid Development**: Python-native web development without frontend complexity
- **Data-Centric**: Built specifically for data applications and analytics
- **Caching System**: Built-in performance optimization for data applications
- **Local Deployment**: Easy local hosting without complex infrastructure
- **Python Ecosystem**: Full access to Python data science libraries

## Architecture Patterns

### Data Flow Architecture
```
Marketplace Reports (CSV/Excel) 
    ↓ (Import & Validation)
DuckDB Tables 
    ↓ (Cross-Marketplace Linking)
Unified Data Model 
    ↓ (Analysis & Processing)
User Interface (Streamlit)
    ↓ (Export & Reporting)
Output Files (Excel/CSV)
```

### Cross-Marketplace Integration
- **Central Linking System**: `CrossMarketplaceLinker` class handles all WB ↔ Ozon connections
- **Barcode-Based Matching**: Primary key for product correlation across platforms
- **Normalized Data Model**: Consistent data structure despite different source formats

### Performance Optimization
- **Caching Strategy**: 
  - `@st.cache_data` for DataFrame operations
  - `@st.cache_resource` for database connections
  - Session state management for heavy operations
- **Memory Management**: 
  - Batch processing for large datasets
  - Streaming for file exports >50MB
  - Session state cleanup after heavy operations

## Development Environment

### Python Environment Management
- **Requirements**: Pinned versions for stability in requirements.txt
- **Local Development**: Virtual environment (venv/) for isolation
- **Testing**: pytest framework with comprehensive fixtures

### Configuration Management
- **Settings**: JSON-based configuration (config.json)
- **Environment Variables**: Support for local customization
- **Default Configuration**: Example configuration (config.example.json)

### Database Management
- **Schema Evolution**: Auto-migration system via `db_migration.py`
- **Data Integrity**: Validation and cleanup utilities
- **Backup Strategy**: File-based backups with timestamps

## Performance Requirements & Constraints

### File Processing Capabilities
- **Large File Support**: Handle marketplace reports up to 200MB
- **Batch Processing**: Efficient processing of thousands of records
- **Memory Management**: Optimize for local machine resource constraints
- **Progress Tracking**: User feedback for long-running operations

### Response Time Expectations
- **Interactive Operations**: <2 seconds for basic queries and searches
- **Complex Analysis**: Acceptable longer processing time for Rich Content generation
- **Large Dataset Operations**: Progress indicators and user feedback
- **Export Operations**: Streaming for large file downloads

### Resource Optimization
- **Database Connection Pooling**: Efficient DuckDB connection management
- **Memory-Safe Operations**: Generators and batch processing for large datasets
- **Disk Space Management**: Efficient data storage and cleanup utilities
- **CPU Utilization**: Multi-threaded operations where applicable

## Integration Constraints

### External Dependencies
- **No Marketplace APIs**: File-based approach for maximum reliability
- **Limited External Services**: Google Sheets integration only
- **Local-First Architecture**: Minimize external dependencies
- **Offline Capability**: Core functionality works without internet

### Data Security & Privacy
- **Local Data Storage**: All sensitive data remains on user's machine
- **No Authentication**: Single-user local deployment model
- **File Permissions**: Appropriate file system security
- **Data Isolation**: No external data transmission

## Best Practices & Standards

### Code Quality Standards
- **Python Best Practices**: PEP 8 style guidelines and modern Python patterns
- **Error Handling**: Comprehensive try-catch blocks with user-friendly messages
- **Logging**: Structured logging for debugging and monitoring
- **Documentation**: Inline documentation and comprehensive docstrings

### Performance Best Practices
- **Database Queries**: Parameterized queries and efficient joins
- **Memory Management**: Regular cleanup and efficient data structures
- **Caching Strategy**: Appropriate use of Streamlit caching decorators
- **Batch Operations**: Process large datasets in manageable chunks

### Development Workflow
- **Testing Strategy**: Unit tests for critical business logic
- **Version Control**: Git-based workflow with appropriate .gitignore
- **Documentation**: Comprehensive project documentation in project-docs/
- **Configuration Management**: Separate configuration from code

## Technology Roadmap Considerations

### Current Technology Limitations
- **Single-User Constraint**: No multi-user capabilities in current stack
- **Local Deployment Only**: No cloud or remote deployment capabilities
- **File-Based Data Sources**: Limited to CSV/XLSX marketplace reports

### Future Technology Opportunities
- **Enhanced Analytics**: More advanced data science libraries
- **Performance Optimization**: Database indexing and query optimization
- **UI/UX Improvements**: More sophisticated Streamlit components
- **Integration Expansion**: Additional marketplace data sources

### Upgrade Path Considerations
- **Backward Compatibility**: Maintain existing data format compatibility
- **Migration Strategy**: Smooth upgrade path for users
- **Performance Monitoring**: Track system performance over time
- **Security Updates**: Regular dependency updates for security