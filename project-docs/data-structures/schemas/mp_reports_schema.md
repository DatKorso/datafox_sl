## Описание Схемы Данных для Отчетов Маркетплейсов (mp_reports_schema.md)

**ВАЖНОЕ ЗАМЕЧАНИЕ:**

Начиная с версии [укажите текущую дату или версию], данный документ (`mp_reports_schema.md`) используется **ИСКЛЮЧИТЕЛЬНО В КАЧЕСТВЕ ДОКУМЕНТАЦИИ И СПРАВОЧНОГО МАТЕРИАЛА**.

Актуальная схема базы данных, включая структуру таблиц, типы данных, правила трансформации и параметры чтения исходных файлов, **ЗАХАРДКОДЕНА** непосредственно в коде приложения, в файле `utils/db_utils.py` в переменной `HARDCODED_SCHEMA`.

Любые изменения в структуре таблиц или логике импорта должны производиться путем модификации переменной `HARDCODED_SCHEMA` в указанном файле. Данный документ следует обновлять для отражения этих изменений, но он не влияет на поведение приложения.

Этот подход выбран для повышения надежности и предсказуемости работы приложения, устраняя зависимость от внешнего файла конфигурации схемы.

---

# Marketplace Reports Loading Schema

This file defines how data should be loaded from various marketplace report files into the DataFox database. Each section describes the mapping between source files and database tables, as well as any required data transformations.

## Ozon Orders (.csv) - Data Loading Instructions

The user specifies the path to the Ozon orders .csv report in the application settings. The application loads data from this CSV file into the database as follows:

* **Target DuckDB Table:** oz_orders
* **CSV Delimiter:** ';'
* **Header Row Number:** 1
* **Data Starts on Row:** 2
* **Columns to Load:** (List the exact CSV column header names and the corresponding target DuckDB table column names)
  * `Номер заказа` -> `oz_order_number`
  * `Номер отправления` -> `oz_shipment_number`
  * `Принят в обработку` -> `oz_accepted_date`
  * `Статус` -> `order_status`
  * `OZON id` -> `oz_sku`
  * `Артикул` -> `oz_vendor_code`
* **Data Type Conversions/Transformations:**
  * `oz_order_number` varchar
  * `oz_shipment_number` varchar
  * `oz_accepted_date` date
  * `oz_status` varchar
  * `oz_sku` int
  * `oz_vendor_code` varchar
* **Pre-Update Action:** Delete all existing Ozon order data before inserting new data

## Ozon Products (.csv) - Data Loading Instructions

The user specifies the path to the Ozon products .csv report in the application settings. The application loads data from this CSV file into the database as follows:

* **Target DuckDB Table:** oz_products
* **CSV Delimiter:** ';'
* **Header Row Number:** 1
* **Data Starts on Row:** 2
* **Columns to Load:** (List the exact CSV column header names and the corresponding target DuckDB table column names)
  * `Артикул` -> `oz_vendor_code`
  * `Ozon Product ID` -> `oz_product_id`
  * `SKU` -> `oz_sku`
  * `Бренд` -> `oz_brand`
  * `Статус товара` -> `oz_product_status`
  * `Видимость на Ozon` -> `oz_product_visible`
  * `Причины скрытия` -> `oz_hiding_reasons`
  * `Доступно к продаже по схеме FBO, шт.` -> `oz_fbo_stock`
  * `Текущая цена с учетом скидки, ₽` -> `oz_actual_price`
* **Data Type Conversions/Transformations:**
  * `oz_vendor_code` varchar (must remove "'" symbol from data)
  * `oz_product_id` int
  * `oz_sku` int
  * `oz_brand` varchar
  * `oz_product_status` varchar
  * `oz_product_visible` varchar
  * `oz_hiding_reasons` varchar 
  * `oz_fbo_stock` int
  * `oz_actual_price` double or float (example of data: "1636.00")
* **Pre-Update Action:** Delete all existing Ozon order data before inserting new data

## Ozon Barcodes (.xlsx) - Data Loading Instructions

The user specifies the path to the Ozon barcodes .xlsx report in the application settings. The application loads data from this Excel file into the database as follows:

* **Target DuckDB Table:** oz_barcodes
* **Sheet Name:** "Штрихкоды"
* **Header Row Number:** 3
* **Data Starts on Row:** 5
* **Columns to Load:** (List the exact Excel column header names and the corresponding target DuckDB table column names)
  * `Артикул товара` -> `oz_vendor_code`
  * `Ozon ID` -> `oz_product_id`
  * `Штрихкод` -> `oz_barcode`
* **Data Type Conversions/Transformations:**
  * `oz_vendor_code` varchar
  * `oz_product_id` int
  * `oz_barcode` varchar
* **Pre-Update Action:** Delete all existing Ozon order data before inserting new data

## Wildberries Products (Folder) - Data Loading Instructions

The user specifies in the application settings the path to a folder containing .xlsx files. All files in this folder are loaded into the DuckDB table (wb_products), and the data from each report is combined (unioned) together.

* **Target DuckDB Table:** wb_products
* **File Pattern:** Pattern to identify files to process, e.g., "*.xlsx" in folder
* **Sheet Name:** "Товары"
* **Header Row Number:** 3
* **Data Starts on Row:** 5
* **Columns to Load:** (List the exact Excel column header names and the corresponding target DuckDB table column names)
  * `Артикул WB` -> `wb_sku`
  * `Категория продавца` -> `wb_category`
  * `Бренд` -> `wb_brand`
  * `Баркод` -> `wb_barcodes`
  * `Размер` -> `wb_size`
* **Data Type Conversions/Transformations:**
  * `wb_sku` int
  * `wb_category` varchar
  * `wb_brand` varchar
  * `wb_barcodes` varchar (can contain list of barcodes through the symbol ";")
  * `wb_size` int
* **Pre-Update Action:** Delete all existing Ozon order data before inserting new data
* **File Grouping:** Union all records from multiple files

## Wildberries Prices (.xlsx) - Data Loading Instructions

The user specifies the path to the Wildberries .xlsx report in the application settings. The application loads data from this Excel file into the database as follows:

* **Target DuckDB Table:** wb_prices
* **Sheet Name:** "Отчет - цены и скидки на товары"
* **Header Row Number:** 1
* **Data Starts on Row:** 2
* **Columns to Load:** (List the exact Excel column header names and the corresponding target DuckDB table column names)
  * `Артикул WB` -> `wb_sku`
  * `Остатки WB` -> `wb_fbo_stock`
  * `Текущая цена` -> `wb_full_price`
  * `Текущая скидка` -> `wb_discount`
* **Data Type Conversions/Transformations:**
  * `wb_sku` int
  * `wb_fbo_stock` int
  * `wb_full_price` int
  * `wb_discount` int
* **Pre-Update Action:** Delete all existing Wildberries prices data before inserting new data