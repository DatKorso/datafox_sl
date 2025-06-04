# Requirements & Features

## System Requirements

*   Python 3.x
*   Streamlit
*   DuckDB
*   Pandas
*   Openpyxl (for .xlsx file support)
*   (Other libraries as specified in `requirements.txt`)

## Feature Descriptions

### Core Features
1.  **Database Initialization & Management:**
    *   Ability to connect to an existing DuckDB database or create a new one.
    *   Display basic database statistics (e.g., number of tables, database size).
2.  **Data Import:**
    *   Upload report files (CSV, XLSX) from Ozon and Wildberries.
    *   Specify paths for report files for automated loading.
    *   Automatic table creation and data insertion based on report types.
3.  **Data Viewing:**
    *   Display raw data from selected database tables.
4.  **Cross-Marketplace Product Search:**
    *   Search for products across Ozon and WB using various criteria (WB SKU, Ozon SKU, Ozon Vendor Code, Barcode).
    *   Display selected information fields for matched products.
    *   Identify common products based on shared barcodes.
5.  **Ozon Order Statistics (`pages/6_Ozon_Order_Stats.py`):**
    *   User can select to search by Ozon SKU or Wildberries SKU.
    *   Input a list of SKUs (Ozon or WB, depending on selection).
    *   If Ozon SKU is selected:
        *   Displays a table with daily Ozon order counts for the last 30 days (excluding 'Отменён' status) for each entered Ozon SKU.
        *   Displays a sum of Ozon orders for the last 14 days for each entered Ozon SKU.
    *   If Wildberries SKU is selected:
        *   Finds all Ozon SKUs linked to the entered WB SKUs via common barcodes.
        *   Displays a summary table showing, for each WB SKU:
            *   The count of linked Ozon SKUs.
            *   Aggregated daily Ozon order counts for the last 30 days for all linked Ozon SKUs.
            *   Aggregated sum of Ozon orders for the last 14 days for all linked Ozon SKUs.
        *   Each row in the WB SKU summary table is expandable to show the detailed daily and 14-day order statistics for each individual linked Ozon SKU.
6.  **Custom Analytic Report Generation (`pages/8_Analytic_Report.py`) - NEW:**
    *   Load custom Excel reports with specific structure (analytic_report sheet).
    *   Process WB SKUs to find related Ozon data through barcode matching.
    *   Automatically fill size distribution data (OZ_SIZE_22 through OZ_SIZE_44).
    *   Calculate size ranges (OZ_SIZES) and total stock aggregation (OZ_STOCK).
    *   Generate order statistics for the last 30 days (ORDERS_TODAY-30 to ORDERS_TODAY-1).
    *   Update original Excel file while preserving structure and creating backup copies.
    *   Validate file structure and provide detailed error reporting.
    *   Support both file upload and configuration-based file paths.

### User Interface
*   Multi-page Streamlit application.
*   Intuitive navigation and clear instructions.
*   User-friendly input fields and data display.

## Business Rules
*   Order statistics should exclude orders with the status "Отменён".
*   The date range for daily order statistics is the last 30 days from the current date.
*   The sum of orders should cover the last 14 days from the current date.
*   Cross-marketplace linking relies on matching barcodes between `wb_products.wb_barcodes` (after normalization) and `oz_barcodes.oz_barcode`.
*   For WB SKU search in Ozon Order Statistics, if a WB SKU has multiple linked Ozon SKUs, their order data is aggregated for the summary view and shown individually in the expanded view.
*   **Custom Analytic Reports:**
    *   Excel files must contain 'analytic_report' sheet with headers in row 7.
    *   Row 8 is ignored (can contain descriptions).
    *   Data processing starts from row 9.
    *   WB_SKU column is mandatory for processing.
    *   Size distribution is based on wb_products.wb_size field.
    *   Backup files are created with timestamp before any modifications.

## Edge Cases
*   No search values entered for Ozon order statistics.
*   No Ozon SKUs found for the entered WB SKU during barcode matching.
*   Database connection issues.
*   Missing `oz_orders` table or required columns (`oz_sku`, `oz_accepted_date`, `order_status`).
*   Missing `wb_products` or `oz_barcodes` tables (or their barcode columns) when searching by WB SKU in Ozon Order Statistics.
*   Invalid date formats in `oz_accepted_date`.
*   **Custom Analytic Reports:**
    *   Missing 'analytic_report' sheet in Excel file.
    *   Incorrect file structure or missing WB_SKU column.
    *   File locked by other applications during processing.
    *   Insufficient disk space for backup creation.
    *   No matching Ozon data for provided WB SKUs.

### Core Application

*   **Multi-Page Navigation:** Sidebar navigation to different sections (Home, Import Reports, Settings, View Data, Cross-Marketplace Search, Ozon Order Statistics, Custom Analytic Reports).
*   **Configuration Management:**
    *   Uses `config.json` to store database path and default report paths.
    *   `Settings` page allows users to view and update these configurations.

### Database Management (`utils/db_*.py`)

*   **Connection:** Establishes and manages connection to a DuckDB database file.
*   **Schema Definition & Creation:**
    *   Schema for marketplace reports is defined (initially in `utils/db_schema.py`, potentially moving to a more structured format like `project-docs/mp_reports_schema.md`).
    *   Tables are automatically created based on the schema if they don't exist.
*   **Data Import (`pages/2_Import_Reports.py`, `utils/db_crud.py`):
    *   Supports importing data from CSV and XLSX files.
    *   Handles specific data transformations (e.g., single quote removal, date conversion, specific column processing based on schema definitions).
    *   Allows users to upload files directly or use pre-configured paths from `config.json` (with a checkbox to select this option for single file reports).
    *   Supports importing all XLSX files from a specified folder for certain report types (e.g., WB stock reports).
    *   Provides feedback on import success/failure and number of records imported.
*   **Data Viewing (`pages/4_View_Data.py`):
    *   Allows users to select a table and view its contents in a paginated manner.
    *   Shows column names and data types.
*   **Database Statistics (`pages/1_Home.py`, `utils/db_crud.py`):
    *   Displays total table count, total record count (for managed tables), and DB file size on the Home page.
    *   Shows record counts for each individual managed table.

### User Interface (Streamlit Pages - `pages/*.py`)

*   **Home Page (`1_Home.py`):
    *   Welcome message.
    *   Database connection status and key analytics.
    *   Prompts for configuration or data import if needed.
*   **Import Reports Page (`2_Import_Reports.py`):
    *   Marketplace selection (Ozon, Wildberries).
    *   Report type selection based on defined schemas for the chosen marketplace.
    *   File upload interface or folder path input.
    *   Checkbox to use default path from settings for single file reports.
    *   Import button and progress display.
    *   Preview of data before import.
*   **Settings Page (`3_Settings.py`):
    *   Displays current database path and report paths from `config.json`.
    *   Allows users to update these paths.
*   **View Data Page (`4_View_Data.py`):
    *   Dropdown to select table.
    *   Data display with pagination controls.
*   **Cross-Marketplace Search Page (`5_Cross_Marketplace_Search.py`):
    *   (Placeholder/Future Feature) Interface for searching data across different tables.
*   **Ozon Order Statistics Page (`6_Ozon_Order_Stats.py`):**
    *   Input for Ozon SKUs or WB SKUs (selectable).
    *   Displays order statistics (daily for 30 days, sum for 14 days).
    *   For WB SKUs, shows aggregated data and expandable details for linked Ozon SKUs.
*   **Custom Analytic Report Page (`8_Analytic_Report.py`) - NEW:**
    *   File selection via upload or configured path.
    *   Excel file structure validation and preview.
    *   Batch processing of WB SKUs with progress tracking.
    *   Comprehensive statistics display after processing.
    *   Error handling with detailed recommendations.
    *   Automatic backup creation and file management.

## Business Rules

*   Report file names or structures might vary; import logic should be robust or adaptable (e.g., via `read_params` in schema).
*   Specific data cleaning rules (e.g., removing single quotes from `oz_vendor_code`) are applied during import based on schema definitions.
*   Date formats from reports are converted to a standard SQL date/timestamp format.

## Edge Cases

*   **Missing Configuration:** Application guides users to the Settings page if DB path is not set.
*   **DB Connection Failure:** Error messages are shown if connection to DB fails.
*   **File Not Found (for configured paths):** Errors are displayed if a configured file path is invalid or the file doesn't exist.
*   **Empty/Malformed Files:** Import process should handle these gracefully, logging errors and skipping problematic files/rows where possible.
*   **Schema Mismatches:** (Future consideration) How to handle files that don't match the expected schema.

## Core Requirements

### 1. Database Management
- **Local DuckDB Integration**: Store and manage marketplace data in a local DuckDB database
- **Database Configuration**: Allow users to configure database path and settings
- **Schema Management**: Automatically create and manage table schemas for different marketplace data types
- **Data Validation**: Validate and clean data during import with detailed error reporting

### 2. Data Import System
- **Multi-Marketplace Support**: Import data from Ozon and Wildberries marketplaces
- **Multiple File Formats**: Support CSV and Excel (.xlsx) file formats
- **Batch Processing**: Handle multiple files and folder-based imports
- **Data Transformation**: Apply necessary transformations during import (date conversion, string cleaning, etc.)
- **Error Handling**: Graceful handling of data conversion errors with detailed logging
- **Preview Functionality**: Show preview of data before import

### 3. Cross-Marketplace Analytics
- **Barcode Matching**: Link products across marketplaces using common barcodes
- **Cross-Marketplace Search**: Find matching products between Ozon and Wildberries
- **Order Statistics**: Calculate and display order statistics for different time periods
- **Stock Management**: Track and display stock levels across marketplaces

### 4. Advertising Campaign Management
- **WB to Ozon Mapping**: Find Ozon SKUs linked to Wildberries SKUs via barcodes
- **Performance Analysis**: Analyze order volume and stock levels for candidate products
- **Automated Selection**: Automatically select top candidates for advertising campaigns
- **Campaign Optimization**: Filter candidates based on stock availability and order performance

### 5. Custom Report Generation - NEW
- **Excel Report Processing**: Load and validate custom Excel reports with specific structure
- **Automated Data Filling**: Automatically populate size distribution, stock, and order data
- **Cross-Reference Analysis**: Map WB SKUs to Ozon data through barcode matching
- **File Management**: Create backups and update original files while preserving structure
- **Batch Processing**: Handle multiple WB SKUs efficiently with progress tracking
- **Error Recovery**: Comprehensive error handling with actionable recommendations

### Data Import & Export
- Import reports from Excel/CSV files
- Support for folder-based imports (multiple files)
- **Google Sheets Integration**
  - Direct import from Google Sheets documents
  - Real-time data synchronization
  - Support for shared and public documents
  - Automatic URL validation and testing

### Punta Data Management
- **Punta Table Support**
  - Import product data from Google Sheets
  - Automatic table cleanup before import
  - Support for product attributes (gender, season, model, material)
  - Integration with existing WB SKU system 