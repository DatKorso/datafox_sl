# Project Structure & Organization

## Root Directory Layout

```
datafox_sl/
├── app.py                     # Main Streamlit application entry point
├── config.json               # Application configuration
├── requirements.txt          # Python dependencies
├── project_schema.sql        # Database schema definition
├── pages/                    # Streamlit pages (numbered for ordering)
├── utils/                    # Shared utility modules
├── data/                     # Database files and reports
├── rich_recommend/           # TypeScript microservice
├── tests/                    # Test suite
├── project-docs/             # Comprehensive documentation
└── marketplace_reports/      # Raw marketplace data files
```

## Key Directories

### `/pages/` - Streamlit Application Pages
Numbered pages for consistent navigation ordering:
- `1_🏠_Главная.py` - Home dashboard
- `2_🖇_Импорт_Отчетов_МП.py` - Data import tools
- `3_⚙️_Настройки.py` - Application settings
- `4_📖_Просмотр_БД.py` - Database viewer
- `5_🔎_Поиск_Между_МП.py` - Cross-marketplace search
- `6-17_*` - Specialized analytics and management tools

### `/utils/` - Shared Utilities
Core utility modules organized by functionality:
- **Database**: `db_connection.py`, `db_crud.py`, `db_schema.py`
- **Cross-marketplace**: `cross_marketplace_linker.py` (central linking logic)
- **Data Processing**: `data_cleaning.py`, `data_cleaner.py`
- **UI Components**: `*_ui_components.py` files for reusable Streamlit components
- **Specialized Tools**: `wb_recommendations.py`, `rich_content_oz.py`

### `/rich_recommend/` - TypeScript Microservice
Modular TypeScript service structure:
```
rich_recommend/src/
├── index.ts                  # Express server entry point
├── data-source.ts           # TypeORM database configuration
├── entity/                  # Database entities
├── modules/                 # Feature modules (recommendations, rich-content, etc.)
├── migration/               # Database migrations
└── public/                  # Static web assets
```

### `/project-docs/` - Documentation
Comprehensive documentation organized by category:
- **User Guides**: Step-by-step usage instructions
- **Technical**: Architecture, implementation details, API docs
- **Data Structures**: Database schemas and relationships
- **Project Management**: Planning, reports, changelogs

## Naming Conventions

### Files
- **Python modules**: `snake_case.py`
- **Streamlit pages**: `{number}_{emoji}_{Russian_Name}.py`
- **TypeScript files**: `PascalCase.ts` for classes, `camelCase.ts` for utilities
- **Documentation**: `kebab-case.md`

### Database Tables
- **Ozon tables**: `oz_*` prefix (e.g., `oz_products`, `oz_barcodes`)
- **Wildberries tables**: `wb_*` prefix (e.g., `wb_products`, `wb_prices`)
- **System tables**: descriptive names (e.g., `marketplace_links`)

### Code Organization
- **Classes**: PascalCase (e.g., `CrossMarketplaceLinker`)
- **Functions**: snake_case (e.g., `get_bidirectional_links`)
- **Constants**: UPPER_SNAKE_CASE
- **Variables**: snake_case

## Import Patterns

### Python Imports
```python
# Standard library first
import os
import json
from typing import List, Dict, Optional

# Third-party libraries
import streamlit as st
import pandas as pd
import duckdb

# Local utilities
from utils.db_connection import connect_db
from utils.cross_marketplace_linker import CrossMarketplaceLinker
```

### Utility Module Structure
Each utility module should follow this pattern:
- Docstring explaining module purpose
- Import statements
- Constants and configuration
- Helper functions
- Main classes
- Public API functions

## Configuration Management

### Config Files
- `config.json` - Main application configuration
- `config.example.json` - Template for new installations
- `.streamlit/config.toml` - Streamlit-specific settings

### Environment Variables
- Database paths and connection strings
- API keys and external service credentials
- Feature flags and debug settings

## Data Flow Architecture

### Input Sources
- Excel files in `marketplace_reports/`
- Google Sheets integration
- Manual data entry through Streamlit interface

### Processing Pipeline
1. **Import** → `pages/2_*` and utility importers
2. **Storage** → DuckDB database in `data/`
3. **Linking** → `CrossMarketplaceLinker` for product relationships
4. **Analysis** → Specialized page modules
5. **Export** → Excel, CSV, or direct database queries

### Cross-Service Communication
- Python ↔ TypeScript via HTTP API calls
- Shared data through database connections
- File-based data exchange for large datasets