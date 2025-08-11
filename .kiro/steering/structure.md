---
inclusion: always
---

# File Organization & Module Structure Rules

## File Placement Rules

### ALWAYS place new files in these directories:
- **Streamlit pages**: `/pages/` with format `{number}_{emoji}_{Russian_Name}.py`
- **Shared utilities**: `/utils/` for reusable functions and classes
- **Database operations**: Use existing `utils/db_*.py` modules or extend them
- **UI components**: Create `*_ui_components.py` files in `/utils/` for reusable Streamlit widgets
- **Tests**: `/tests/unit/` for unit tests, `/tests/` for integration tests

### NEVER create files in:
- Root directory (except configuration files)
- `/data/` directory (reserved for database files)
- `/marketplace_reports/` (reserved for raw data files)

## Mandatory Naming Conventions

### Python Files
- **Modules**: `snake_case.py` (e.g., `wb_recommendations.py`)
- **Classes**: `PascalCase` (e.g., `CrossMarketplaceLinker`)
- **Functions**: `snake_case` (e.g., `get_bidirectional_links`)
- **Constants**: `UPPER_SNAKE_CASE`
- **Variables**: `snake_case`

### Streamlit Pages
- **Format**: `{number}_{emoji}_{Russian_Name}.py`
- **Numbering**: Sequential (1-17 currently used)
- **Emojis**: Single emoji representing page function
- **Names**: Russian descriptive names with underscores

### Database Tables
- **Ozon**: MUST use `oz_` prefix (e.g., `oz_products`, `oz_barcodes`)
- **Wildberries**: MUST use `wb_` prefix (e.g., `wb_products`, `wb_prices`)
- **System**: Descriptive names (e.g., `marketplace_links`)

## Module Structure Patterns

### Utility Module Template
ALWAYS structure utility modules in this order:
```python
"""Module docstring explaining purpose."""

# Standard library imports
import os
from typing import List, Dict, Optional

# Third-party imports
import pandas as pd
import streamlit as st

# Local imports
from utils.db_connection import connect_db

# Constants
DEFAULT_BATCH_SIZE = 1000

# Helper functions
def _private_helper():
    pass

# Main classes
class MainClass:
    pass

# Public API functions
def public_function():
    pass
```

### Streamlit Page Template
ALWAYS structure pages with:
1. Page configuration (`st.set_page_config`)
2. Imports (following standard order)
3. Helper functions
4. Main page logic
5. Sidebar components
6. Error handling with `st.error()`, `st.warning()`

## Import Order Rules

### ALWAYS follow this import order:
```python
# 1. Standard library
import os
import json
from typing import List, Dict, Optional

# 2. Third-party libraries
import streamlit as st
import pandas as pd
import duckdb

# 3. Local utilities (alphabetical)
from utils.db_connection import connect_db
from utils.cross_marketplace_linker import CrossMarketplaceLinker
```

### Database Connection Rules
- **ALWAYS** use `from utils.db_connection import connect_db`
- **NEVER** create direct database connections in pages
- **ALWAYS** use context managers for database operations

## UI Component Organization

### Reusable Components
- **MUST** create `*_ui_components.py` files for shared Streamlit widgets
- **ALWAYS** prefix component functions with the module name
- **EXAMPLE**: `cards_matcher_ui_components.py` → `cards_matcher_display_results()`

### Page-Specific Components
- **SHOULD** define helper functions within the page file
- **MUST** use descriptive names starting with `_` for private helpers
- **ALWAYS** place UI helpers before main page logic

## Cross-Service Integration

### TypeScript Service Communication
- **ALWAYS** use HTTP API calls for Python ↔ TypeScript communication
- **NEVER** share database connections between services
- **MUST** handle API errors gracefully with user feedback

### File Processing Rules
- **ALWAYS** validate file uploads before processing
- **MUST** use streaming for files >10MB
- **SHOULD** provide progress indicators for long operations
- **ALWAYS** clean up temporary files after processing