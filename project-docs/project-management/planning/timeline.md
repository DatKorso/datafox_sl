# Project Timeline & Progress

## Phases & Milestones

*   **Phase 1: Initial Setup & Core DB Logic (Completed)**
    *   [X] Project scaffolding (folders, `main_app.py`, `requirements.txt`).
    *   [X] Configuration management (`config.json`, `utils/config_utils.py`).
    *   [X] Initial unified database utilities (`utils/db_utils.py` - later refactored).
        *   [X] DB Connection.
        *   [X] Hardcoded schema definition.
        *   [X] Table creation based on schema.
        *   [X] Basic data import logic (CSV, XLSX) with initial transformations.
        *   [X] DB statistics retrieval.
    *   [X] Basic `pages/1_Home.py` with DB stats display.
    *   [X] Basic `pages/2_Import_Reports.py` for uploading files.
    *   [X] Basic `pages/3_Settings.py` for path configuration.
    *   [X] Basic `pages/4_View_Data.py` for table viewing.

*   **Phase 2: Refinement and Feature Enhancement (Partially Completed)**
    *   [X] Refactor `utils/db_utils.py` into modular components:
        *   [X] `utils/db_connection.py`
        *   [X] `utils/db_schema.py` (including schema definition moved here)
        *   [X] `utils/db_crud.py` (import logic, data querying, stats)
    *   [X] Update all page imports and function calls to use refactored DB utilities.
    *   [X] Improve `pages/2_Import_Reports.py`:
        *   [X] Marketplace and report type selection based on dynamic schema.
        *   [X] Support for folder-based XLSX import (e.g., WB stock).
        *   [X] Data preview before import.
        *   [X] Checkbox to use default path from settings for single file reports (Current task).
    *   [ ] Enhance `pages/4_View_Data.py` with pagination and data type display.
    *   [ ] Implement `pages/5_Cross_Marketplace_Search.py` (initial design).
    *   [X] Create `project-docs/` directory and populate initial documentation.
        *   [X] `overview.md`
        *   [X] `requirements.md`
        *   [X] `tech-specs.md`
        *   [X] `user-structure.md`
        *   [X] `timeline.md`
    *   [X] Create `pages/6_Ozon_Order_Stats.py` with:
        *   [X] Search by Ozon SKU for 30-day order stats.
        *   [X] Search by WB SKU, linking to Ozon SKUs, with aggregated and expandable detailed stats.

*   **Phase 3: Advanced Features & Polish (Future)**
    *   [ ] Advanced error handling and logging for imports.
    *   [ ] More robust data validation during import.
    *   [ ] UI/UX improvements across all pages.
    *   [ ] Comprehensive testing (unit, integration).
    *   [ ] (If needed) Schema migration capabilities.
    *   [ ] Full implementation of Cross-Marketplace Search with various criteria.

*   **Phase 4: Google Sheets Integration (Latest)**
    **Completed**: January 2025
    - ✅ Google Sheets API integration via CSV export
    - ✅ New punta_table schema and data structure  
    - ✅ Enhanced Settings page with Google Sheets configuration
    - ✅ Updated Import Reports page with Google Sheets support
    - ✅ Comprehensive documentation and user guides
    - ✅ URL validation and testing utilities

    **Features Added:**
    - Direct import from Google Sheets documents
    - Automatic table cleanup before import (punta_table)
    - Real-time data validation and testing
    - Support for shared and public Google Sheets documents
    - Enhanced error handling and user feedback

## Progress Tracking

*   **Current Date:** (To be filled by user/assistant at time of update)
*   **Last Update:** (To be filled by user/assistant at time of update) - Added checkbox for using configured paths in Import Reports page. Updated project documentation.
*   **Next Steps:**
    *   Verify functionality of the "use path from settings" checkbox.
    *   Continue with Phase 2 tasks, focusing on `View Data` page enhancements and `Cross-Marketplace Search` initial implementation.
    *   Test Ozon Order Statistics page with various data scenarios.

## Change Records

*   **[Date of your last major interaction summary]**: Refactored `db_utils.py` into `db_connection.py`, `db_schema.py`, and `db_crud.py`. Updated pages to use new utilities. Added dynamic report selection and folder import to `Import_Reports.py`.
*   **[Current Date/This interaction]**: Added a checkbox to `Import_Reports.py` to allow users to opt for using a pre-configured file path from settings instead of uploading a new file for single-file report types. Created and populated initial project documentation files in `project-docs/`.
*   **[Current Date/This interaction]**: Fixed `read_excel()` error by removing unsupported `data_starts_on_row` parameter from `read_params` before passing to pandas in `pages/2_Import_Reports.py`.
*   **[Current Date/This interaction]**: Implemented logic to skip a specified number of rows after the header for Excel imports (e.g., row 4 in `oz_barcodes` and `wb_products`) using a new `skip_rows_after_header` parameter in schema and corresponding processing in `pages/2_Import_Reports.py`.
*   **[Current Date/This interaction]**: Created `pages/6_Ozon_Order_Stats.py` for viewing Ozon order statistics by Ozon SKU or linked WB SKU. Updated all project documentation. Fixed a database connection closure issue on this new page.
*   **[28/05/2025]**: Получена информация о заказах по Ozon SKU `836057912` из таблицы `oz_orders` с использованием SQL-запроса.
*   **[28/05/2025]**: Исправлена ошибка на странице `6_Ozon_Order_Stats.py`, из-за которой статистика по Ozon SKU отображалась некорректно. Проблема была связана с преобразованием SKU в целые числа вместо использования строк в SQL-запросе.
*   **[Текущая дата]**: Проведен комплексный анализ структуры проекта. Создан детальный отчет (`analysis-report.md`) и план улучшений (`improvement-roadmap.md`). Выявлены сильные стороны архитектуры и области для развития.
