# Requirements Document

## Introduction

This feature enhancement adds comprehensive margin calculation functionality to the Ozon advertising manager (pages/7_🎯_Менеджер_Рекламы_OZ.py). The system will calculate and display margin percentages for advertising candidates based on configurable business parameters and cost data from the Punta reference table.

## Requirements

### Requirement 1

**User Story:** As a marketplace analyst, I want to configure margin calculation parameters in the application settings, so that I can customize the calculation based on current business conditions.

#### Acceptance Criteria

1. WHEN I access the application settings THEN the system SHALL provide input fields for five margin calculation parameters
2. WHEN I configure commission parameter THEN the system SHALL accept percentage values with default value of 36%
3. WHEN I configure acquiring parameter THEN the system SHALL accept percentage values with default value of 0%
4. WHEN I configure advertising parameter THEN the system SHALL accept percentage values with default value of 3%
5. WHEN I configure VAT parameter THEN the system SHALL accept percentage values with default value of 20%
6. WHEN I configure exchange rate parameter THEN the system SHALL accept numeric values with default value of 90
7. WHEN I save configuration changes THEN the system SHALL persist the values in config.json
8. WHEN the application starts THEN the system SHALL load the saved configuration values

### Requirement 2

**User Story:** As a marketplace analyst, I want the system to retrieve cost price data from the Punta table, so that margin calculations can be performed accurately.

#### Acceptance Criteria

1. WHEN processing WB SKUs for margin calculation THEN the system SHALL query the punta_table for cost_price_usd values
2. WHEN cost_price_usd data is available THEN the system SHALL use the first occurrence for each WB SKU
3. WHEN cost_price_usd data is missing for a WB SKU THEN the system SHALL handle the case gracefully and indicate unavailable data
4. WHEN multiple records exist for the same WB SKU THEN the system SHALL use ROW_NUMBER() OVER (PARTITION BY wb_sku ORDER BY ROWID) to get the first occurrence
5. WHEN querying punta_table THEN the system SHALL ensure proper data type conversion for wb_sku matching

### Requirement 3

**User Story:** As a marketplace analyst, I want the system to calculate margin percentages using the specified formula, so that I can evaluate the profitability of advertising candidates.

#### Acceptance Criteria

1. WHEN all required data is available THEN the system SHALL calculate margin using the formula: (((oz_actual_price/(1+VAT)-(oz_actual_price*((Commission+Acquiring+Advertising)/100))/1.2)/ExchangeRate)-cost_price_usd)/cost_price_usd
2. WHEN VAT is configured as percentage THEN the system SHALL convert it to decimal (VAT/100) for the formula
3. WHEN commission, acquiring, and advertising are configured as percentages THEN the system SHALL use them directly in the formula
4. WHEN calculation results in a valid number THEN the system SHALL format the result as a percentage with 1 decimal place
5. WHEN calculation results in division by zero or invalid data THEN the system SHALL return "N/A" or appropriate error indicator
6. WHEN cost_price_usd is zero or missing THEN the system SHALL return "Нет данных" for that record

### Requirement 4

**User Story:** As a marketplace analyst, I want to see calculated margin percentages in the advertising candidates table, so that I can make informed decisions about which products to advertise.

#### Acceptance Criteria

1. WHEN displaying advertising candidates THEN the system SHALL replace the "скоро" placeholder with calculated margin percentages
2. WHEN margin calculation is successful THEN the system SHALL display the percentage with "%" symbol and 1 decimal place
3. WHEN margin data is unavailable THEN the system SHALL display "Нет данных" in the margin column
4. WHEN margin is negative THEN the system SHALL display the value in red color to indicate loss
5. WHEN margin is positive THEN the system SHALL display the value in green color to indicate profit
6. WHEN margin is between 0-10% THEN the system SHALL display the value in orange color to indicate low margin

### Requirement 5

**User Story:** As a marketplace analyst, I want the margin calculation to integrate seamlessly with the existing advertising manager workflow, so that I can use the feature without disrupting my current process.

#### Acceptance Criteria

1. WHEN I search for advertising candidates THEN the system SHALL automatically calculate margins for all results
2. WHEN I apply selection criteria THEN the margin calculation SHALL work with filtered results
3. WHEN I export results THEN the margin data SHALL be included in all export formats
4. WHEN the system encounters database errors THEN the system SHALL log errors appropriately and continue processing other records
5. WHEN configuration is missing THEN the system SHALL use default values and display a warning message
6. WHEN punta_table is unavailable THEN the system SHALL display appropriate error message and continue with other functionality

### Requirement 6

**User Story:** As a system administrator, I want comprehensive error handling and logging for margin calculations, so that I can troubleshoot issues and ensure system reliability.

#### Acceptance Criteria

1. WHEN margin calculation fails for individual records THEN the system SHALL log the error with context (WB SKU, Ozon SKU, error details)
2. WHEN database queries fail THEN the system SHALL display user-friendly error messages in Russian
3. WHEN configuration values are invalid THEN the system SHALL validate and provide feedback to the user
4. WHEN punta_table structure changes THEN the system SHALL handle missing columns gracefully
5. WHEN calculation produces extreme values THEN the system SHALL validate results and flag suspicious data
6. WHEN system performance is impacted THEN the system SHALL implement appropriate caching for configuration values