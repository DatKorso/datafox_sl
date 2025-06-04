## Features

- 📊 **Multi-marketplace Data Analysis**: Import and analyze data from Ozon and Wildberries
- 🔍 **Cross-marketplace Search**: Find linked products via common barcodes
- 🎯 **Advertising Campaign Management**: Automated candidate selection for Ozon campaigns
- 📈 **Performance Analytics**: Order statistics and stock analysis
- 🔄 **Data Import/Export**: Support for Excel, CSV, and Google Sheets
- ⚙️ **Flexible Configuration**: Customizable paths and settings
- 🚀 **Real-time Integration**: Direct import from Google Sheets documents
- 📋 **Custom Analytic Reports**: Automated Excel report generation with cross-marketplace data (NEW!)

## Quick Start

### Custom Analytic Reports (New!)
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

### Google Sheets Integration (New!)
1. **Prepare your Google Sheets document**:
   - Ensure public access or "anyone with link can view"
   - Use headers: wb_sku, gender, season, model_name, material, new_last, mega_last, best_last

2. **Configure in application**:
   - Go to Settings → Google Sheets Integration
   - Paste your Google Sheets URL
   - Test connection and preview data

3. **Import data**:
   - Go to Import Reports → Select "punta_table"
   - Click Import to sync data automatically 