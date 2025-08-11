# Implementation Plan

- [x] 1. Extend configuration system for margin parameters
  - Add margin_calculation section to DEFAULT_CONFIG in utils/config_utils.py
  - Create helper functions for getting and setting margin configuration values
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8_

- [x] 2. Add margin configuration UI to settings page
  - Create new section "Margin Calculation Parameters" in pages/3_⚙️_Настройки.py
  - Add input fields for commission, acquiring, advertising, VAT, and exchange rate
  - Integrate with existing save/load settings functionality
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8_

- [x] 3. Implement cost price data retrieval from punta_table
  - Create get_cost_prices_from_punta function in pages/7_🎯_Менеджер_Рекламы_OZ.py
  - Handle missing punta_table gracefully with appropriate error messages
  - Return DataFrame with wb_sku and cost_price_usd columns
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

- [x] 4. Implement margin calculation function
  - Create calculate_margin_percentage function with the specified formula
  - Handle edge cases like division by zero and missing cost data
  - Return formatted percentage or error indicator
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6_

- [x] 5. Integrate margin calculation into advertising manager workflow
  - Modify get_linked_ozon_skus_with_details to include cost price data
  - Add margin calculation to the data processing pipeline
  - Replace "скоро" placeholder with calculated margin values
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6_

- [x] 6. Add basic error handling and validation
  - Implement graceful handling of missing cost data
  - Add configuration validation with default fallbacks
  - Ensure system continues working when punta_table is unavailable
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6_