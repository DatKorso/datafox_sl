## Features

- 📊 **Multi-marketplace Data Analysis**: Import and analyze data from Ozon and Wildberries
- 🔍 **Cross-marketplace Search**: Find linked products via common barcodes
- 🎯 **Advertising Campaign Management**: Automated candidate selection for Ozon campaigns
- 📈 **Performance Analytics**: Order statistics and stock analysis
- 🔄 **Data Import/Export**: Support for Excel, CSV, and Google Sheets
- ⚙️ **Flexible Configuration**: Customizable paths and settings
- 🚀 **Real-time Integration**: Direct import from Google Sheets documents
- 📋 **Custom Analytic Reports**: Automated Excel report generation with cross-marketplace data
- 🔄 **Universal Punta Table**: Dynamic schema adaptation for any Google Sheets structure (NEW!)

## Quick Start

### Universal Punta Table (New!)
The system now supports a universal `punta_table` that automatically adapts to any Google Sheets structure:

1. **Prepare your Google Sheets**:
   - First row: Column headers (any names you want)
   - Required: `wb_sku` column for linking with other data
   - Optional: Any additional columns (gender, season, model_name, etc.)

2. **Import process**:
   - Go to Settings → Google Sheets Integration
   - Paste your Google Sheets URL
   - Go to Import Reports → Select "punta_table"
   - Click Import - the system will automatically:
     - Detect your column structure
     - Create table schema dynamically
     - Convert wb_sku to INTEGER format
     - Clean and validate data

3. **Use in reports**:
   - All columns become available as PUNTA_columnname in Excel reports
   - Example: `season` column → `PUNTA_season` in Excel
   - Automatic detection and filling of all PUNTA_ columns

### Custom Analytic Reports
1. **Prepare your Excel file**:
   - Create a file with 'analytic_report' sheet
   - Row 7: Headers (N, WB_SKU, OZ_SIZE_27, OZ_SIZE_28, ..., OZ_SIZES, OZ_STOCK, ORDERS_TODAY-30, ...)
   - Row 8: Descriptions (ignored during processing)
   - Rows 9+: Your WB SKU data

2. **Configure in application**:
   - Go to Settings → Custom Reports
   - Set path to your analytic report file
   - Or use file upload feature

3. **Process report**:
   - Go to Custom Analytic Report Generation
   - Select your file or use configured path
   - Click "Process Report" to automatically fill all data
   - System creates backup and updates original file

### Google Sheets Integration
1. **Prepare your Google Sheets document**:
   - Ensure public access or "anyone with link can view"
   - First row: Column headers
   - Required: wb_sku column
   - Optional: Any additional data columns

2. **Configure in application**:
   - Go to Settings → Google Sheets Integration
   - Paste your Google Sheets URL
   - Test connection and preview data

3. **Import data**:
   - Go to Import Reports → Select "punta_table"
   - Click Import to sync data automatically with dynamic schema detection 