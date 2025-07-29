# DataFox SL - Marketplace Analytics Platform

DataFox SL is a comprehensive marketplace analytics platform for analyzing data from Russian marketplaces Ozon and Wildberries. The system provides tools for product card management, order statistics, cross-marketplace search, and automated content generation.

## Core Features

- **Cross-marketplace linking**: Links products between Ozon and Wildberries using barcodes
- **Analytics & reporting**: Order statistics, sales analysis, and automated Excel report generation  
- **Product management**: Card matching, category comparison, and quality control tools
- **Content generation**: Rich content generation for Ozon product cards and WB recommendations
- **Data processing**: Import/export tools for marketplace reports and data management

## Target Users

- Marketplace sellers managing products across Ozon and Wildberries
- Analysts performing deep data analysis and trend identification
- Managers requiring strategic planning and comprehensive reporting

## Architecture

The system uses a hybrid architecture:
- **Python/Streamlit** for the main web interface and data processing
- **DuckDB** as the embedded analytical database
- **TypeScript/Node.js** microservice (`rich_recommend`) for advanced AI processing and recommendations
- **PostgreSQL** for the TypeScript service data persistence