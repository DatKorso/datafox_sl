# User Flow & Project Structure

## User Journey

1.  **Initial Setup / First Run:**
    *   User launches the Streamlit application (`streamlit run main_app.py`).
    *   Lands on the **Home Page**.
    *   If the database path is not configured in `config.json` or the DB file doesn't exist, the Home page displays a warning and a button to navigate to the **Settings Page**.
2.  **Configuration (Settings Page):**
    *   User navigates to the **Settings Page**.
    *   Views current settings (DB path, report paths).
    *   Modifies the database path (e.g., `data/marketplace.db`).
    *   (Optionally) Modifies default paths for various marketplace reports.
    *   Saves settings. The `config.json` file is updated.
    *   If a new DB path is set and the file doesn't exist, the system doesn't automatically create it here; creation is handled by initial connection attempts and schema setup on other pages (e.g., Home, Import).
3.  **Importing Data (Import Reports Page):**
    *   User navigates to the **Import Reports Page**.
    *   Selects the marketplace (Ozon or Wildberries) from the sidebar.
    *   Selects the specific report type to import from the sidebar (e.g., Ozon Products, WB Sales).
    *   The main area updates to show options for the selected report:
        *   If it's a single file report type (e.g., Ozon Products CSV):
            *   If a default path is configured in `config.json` and the file exists, a checkbox "Use path from settings: [path]" appears, checked by default.
            *   If the checkbox is checked, the application will use the configured path.
            *   If unchecked, or if no valid default path is set, a file uploader (`st.file_uploader`) is displayed for the user to manually upload the file.
        *   If it's a folder-based report type (e.g., WB Product Stock XLSX from folder):
            *   A text input (`st.text_input`) appears, pre-filled with the default path from `config.json` if available. The user can confirm or change this path.
    *   User provides the file/path and clicks the "Import" button.
    *   The application reads the file(s), processes data according to the schema in `utils/db_schema.py` (transformations, column mapping).
    *   Data cleaning and validation is performed with detailed reporting of any issues.
    *   A preview of the first few rows of data to be imported is shown.
    *   Data is loaded into the corresponding table in the DuckDB database. Tables are created if they don't exist.
    *   Feedback is provided (success/failure, number of records imported).
4.  **Viewing Data (View Data Page):**
    *   User navigates to the **View Data Page**.
    *   Selects a table from a dropdown list of available tables in the database.
    *   The content of the selected table is displayed in a scrollable, paginated format.
    *   Column names and data types are shown.
5.  **Reviewing Analytics (Home Page):**
    *   User navigates back to the **Home Page**.
    *   Sees updated database statistics (total tables, total records, DB file size, record counts per table).
6.  **Cross-Marketplace Search (Cross-Marketplace Search Page):**
    *   User navigates to the **Cross-Marketplace Search Page**.
    *   Selects search criterion (WB SKU, Ozon SKU, Ozon Vendor Code, or Barcode).
    *   Enters search values in the text area.
    *   Selects which information fields to display in results.
    *   Clicks "Найти совпадающие товары".
    *   Views results table showing matched products across marketplaces.
7.  **Ozon Order Statistics (Ozon Order Statistics Page):**
    *   User navigates to the **Ozon Order Statistics Page**.
    *   Selects search type (Ozon SKU or WB SKU).
    *   Enters SKUs in the text area.
    *   Clicks "Получить статистику".
    *   If Ozon SKU search: Views a table of Ozon SKUs with their 14-day order sum and daily order counts for the past 29 days.
    *   If WB SKU search: Views a summary table for each WB SKU (linked Ozon SKU count, aggregate 14-day sum, aggregate daily counts). Each WB SKU row is expandable to show detailed stats for individual linked Ozon SKUs.
8.  **Ozon RK Manager (Advertising Campaign Management):**
    *   User navigates to the **Ozon RK Manager Page**.
    *   Enters list of WB SKUs in the text area.
    *   Sets advertising selection criteria (minimum stock, maximum candidates).
    *   Clicks "Найти связанные артикулы".
    *   Views table showing all linked Ozon SKUs grouped by WB SKU with stock levels and order statistics.
    *   Clicks "Отобрать кандидатов для рекламы" to get top recommendations.
    *   Views recommended Ozon SKUs for advertising campaigns based on order volume and stock availability.
    *   Copies selected Ozon SKUs for use in advertising systems.

## User Workflow - Google Sheets Integration

### Setup Process:
1. **Prepare Google Sheets Document**
   - Create or open Google Sheets document
   - Ensure first row contains column headers: wb_sku, gender, season, model_name, material, new_last, mega_last, best_last
   - Set sharing permissions to "Anyone with the link can view"

2. **Configure in Application**
   - Navigate to Settings page
   - Scroll to "Google Sheets Integration" section
   - Paste Google Sheets URL
   - Test connection using "Test Google Sheets" button
   - Preview data using "Preview Data" button
   - Save settings

3. **Import Data**
   - Navigate to Import Reports page
   - Select "punta_table" from report types
   - Verify Google Sheets URL is displayed
   - Click "Import" button
   - Confirm successful import

### Data Flow:
```
Google Sheets Document → CSV Export URL → Data Import → Database Table (punta_table) → Analysis Tools
```

## Data Flow

1.  **Configuration (`config.json`):** Settings (DB path, report paths) are read by `utils/config_utils.py` and used across the application.
2.  **User Input (Files/Paths):** Provided on the **Import Reports Page**.
3.  **Schema Definition (`utils/db_schema.py`):** Defines table structures, data types, transformations, and file read parameters.
4.  **Import Process:**
    *   Files/paths are received by `pages/2_Import_Reports.py`.
    *   Pandas reads data from CSV/XLSX files.
    *   Transformations from the schema are applied.
    *   `utils/db_crud.py` (`import_data_from_dataframe`) loads the Pandas DataFrame into the specified DuckDB table.
    *   `utils/db_connection.py` manages the DuckDB connection.
    *   `utils/db_schema.py` (`create_tables_from_schema`) ensures tables exist before import.
5.  **Data Storage (DuckDB):** The database file (e.g., `data/marketplace.db`) stores all imported and transformed data.
6.  **Data Retrieval:**
    *   For **View Data Page** and **Home Page statistics**, `utils/db_crud.py` functions query the DuckDB database.
    *   For **Cross-Marketplace Search Page** and **Ozon Order Statistics Page (WB SKU search)**, `utils/db_search_helpers.py` is used to find links via barcodes.
    *   For **Ozon Order Statistics Page**, specific logic within the page queries `oz_orders` and processes data.
    *   Data is typically fetched into Pandas DataFrames for display in Streamlit.

## Project File Structure

```
datafox_sl/
├── data/                             # Default directory for database files (e.g., marketplace.db)
│   └── marketplace.db                # Example DuckDB database file
├── marketplace_reports/              # Optional: Default base for storing report files if organized by user
│   ├── ozon/
│   └── wb/
├── pages/                            # Streamlit pages
│   ├── 1_Home.py
│   ├── 2_Import_Reports.py
│   ├── 3_Settings.py
│   ├── 4_View_Data.py
│   └── 5_Cross_Marketplace_Search.py
│   └── 6_Ozon_Order_Stats.py
│   └── 7_Ozon_RK_manager.py
├── project-docs/                     # Project documentation
│   ├── overview.md
│   ├── requirements.md
│   ├── tech-specs.md
│   ├── user-structure.md  <-- This file
│   └── timeline.md
├── utils/                            # Utility modules
│   ├── __init__.py
│   ├── config_utils.py               # For loading/saving config.json
│   ├── db_connection.py            # For establishing DB connection
│   ├── db_crud.py                  # For CRUD operations (import, query, stats)
│   ├── db_schema.py                # For defining and creating table schemas
│   └── ui_utils.py                 # (Optional) For common UI elements/styling
│   └── db_search_helpers.py        # For cross-marketplace linking and related searches
├── venv/                             # Virtual environment (recommended)
├── .gitignore
├── config.json                       # Configuration file (DB path, report paths)
├── main_app.py                       # Main Streamlit application entry point
├── requirements.txt                  # Python package dependencies
└── README.md                         # General project README (distinct from project-docs)
```