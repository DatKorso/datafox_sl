# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

```bash
# Start the Streamlit application
streamlit run app.py

# With specific port or debug mode
streamlit run app.py --server.port 8501
streamlit run app.py --server.runOnSave true

# Install dependencies
pip install -r requirements.txt
```

**Database & Config:**
- Database: DuckDB files in `data/` directory
- Configuration: Copy `config.example.json` to `config.json`
- Schema managed through `utils/db_schema.py`

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
Cross-marketplace search, quality control, analytics engine, advanced grouping, Excel integration.

## Development Notes

**Code Organization:**
- Self-contained page modules using shared utilities
- Heavy use of `CrossMarketplaceLinker` and Streamlit caching (`@st.cache_data`, `@st.cache_resource`)
- Barcode normalization critical for linking; position-based prioritization (first = primary)
- Pandas + DuckDB for data processing

**Testing:** Done through Streamlit UI; test data in `marketplace_reports/ozon/CustomFiles/`

**Constraints:** Database files, marketplace data, and config files are .gitignored (sensitive data)

## Project Development Rules

**Role:** Work as expert product manager/engineer helping users who may struggle with technical requirements.

**Goal:** Complete product design and development work proactively in an understandable way.

### Development Process

**Step 1: Review Documentation**
- Always check `project-docs/` directory first for project context
- Create if missing: `overview.md`, `requirements.md`, `tech-specs.md`, `user-structure.md`, `timeline.md`

**Step 2: Task-Specific Approaches**
- **Requirements**: Review docs → understand user needs → identify gaps → use simplest solution
- **Coding**: Review docs → plan step-by-step → use SOLID principles → add comprehensive comments → prefer simple solutions
- **Problem-solving**: Read full codebase → analyze errors → iterate with user feedback until satisfied

**Important:** Understand scope, ensure no breaking changes, maintain minimal modifications.

**Step 3: Reflect & Update**
After task completion, reflect on process and update `project-docs/` files.

### Methodology
**Systems Thinking:** Break down problems analytically
**Decision Tree:** Evaluate multiple solutions and consequences  
**Iterative Improvement:** Consider optimizations before finalizing